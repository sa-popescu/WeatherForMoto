import type { CustomLayerInterface, CustomRenderMethodInput, MapLibreMap } from 'maplibre-gl';
import { mercatorBox, type FieldGeometry } from './mercator';
import { paletteTexture } from './rainPalette';
import type { RainField } from './rainValues';

// The rain on the map, drawn by the graphics card from rain values
// (rainValues.ts) rather than shown as a picture. For every pixel of the
// screen the field is read smoothly (a cubic B-spline over its cells) and
// only then coloured (rainPalette.ts), so edges stay smooth at any zoom and
// light rain fades out instead of ending in a fringe. A new moment does not
// cross-fade two pictures: the values themselves move from one moment to the
// next, so a shower grows or shrinks instead of showing twice.

type Box = ReturnType<typeof mercatorBox>;

interface Entry {
  box: Box;
  width: number;
  height: number;
  field: RainField;
  /** On the graphics card, once uploaded (again after a lost context). */
  texture: WebGLTexture | null;
}

interface Program {
  program: WebGLProgram;
  vao: WebGLVertexArrayObject;
  buffer: WebGLBuffer;
  palette: WebGLTexture;
  uniforms: Record<(typeof UNIFORMS)[number], WebGLUniformLocation | null>;
}

/** Fields kept on the graphics card, so a step back along the band needs no upload. */
const KEPT = 6;

const UNIFORMS = ['u_matrix', 'u_now', 'u_before', 'u_palette', 'u_nowMap', 'u_beforeMap', 'u_nowSize', 'u_beforeSize', 'u_mix', 'u_opacity'] as const;

const VERTEX = `#version 300 es
in vec2 a_pos;
uniform mat4 u_matrix;
out vec2 v_pos;
void main() {
  v_pos = a_pos;
  gl_Position = u_matrix * vec4(a_pos, 0.0, 1.0);
}`;

const FRAGMENT = `#version 300 es
precision highp float;
in vec2 v_pos;
uniform sampler2D u_now;
uniform sampler2D u_before;
uniform sampler2D u_palette;
uniform vec4 u_nowMap;
uniform vec4 u_beforeMap;
uniform vec2 u_nowSize;
uniform vec2 u_beforeSize;
uniform float u_mix;
uniform float u_opacity;
out vec4 fragColor;

// Rain strength with a cubic B-spline, from four bilinear reads instead of sixteen (GPU Gems 2, chapter 20).
float strengthAt(sampler2D field, vec2 uv, vec2 size) {
  vec2 p = uv * size - 0.5;
  vec2 f = fract(p);
  vec2 base = p - f;
  vec2 f2 = f * f;
  vec2 f3 = f2 * f;
  vec2 w0 = (1.0 - 3.0 * f + 3.0 * f2 - f3) / 6.0;
  vec2 w1 = (4.0 - 6.0 * f2 + 3.0 * f3) / 6.0;
  vec2 w2 = (1.0 + 3.0 * f + 3.0 * f2 - 3.0 * f3) / 6.0;
  vec2 w3 = f3 / 6.0;
  vec2 g0 = w0 + w1;
  vec2 g1 = w2 + w3;
  vec2 h0 = (base - 1.0 + w1 / g0 + 0.5) / size;
  vec2 h1 = (base + 1.0 + w3 / g1 + 0.5) / size;
  float a = texture(field, vec2(h0.x, h0.y)).r;
  float b = texture(field, vec2(h1.x, h0.y)).r;
  float c = texture(field, vec2(h0.x, h1.y)).r;
  float d = texture(field, vec2(h1.x, h1.y)).r;
  return g0.y * (g0.x * a + g1.x * b) + g1.y * (g0.x * c + g1.x * d);
}

// Strength and snow at this pixel; the field fades out over its last cell, so its edge never shows as a line.
vec2 valueAt(sampler2D field, vec4 placement, vec2 size) {
  vec2 uv = v_pos * placement.xy + placement.zw;
  vec2 inside = min(uv, 1.0 - uv) * size;
  float edge = clamp(min(inside.x, inside.y), 0.0, 1.0);
  if (edge <= 0.0) return vec2(0.0);
  return vec2(strengthAt(field, uv, size) * edge, texture(field, uv).g);
}

void main() {
  vec2 value = valueAt(u_now, u_nowMap, u_nowSize);
  if (u_mix < 1.0) value = mix(valueAt(u_before, u_beforeMap, u_beforeSize), value, u_mix);
  float x = (value.x * 255.0 + 0.5) / 256.0;
  vec4 colour = mix(texture(u_palette, vec2(x, 0.25)), texture(u_palette, vec2(x, 0.75)), value.y);
  float alpha = colour.a * u_opacity;
  fragColor = vec4(colour.rgb * alpha, alpha);
}`;

function compile(gl: WebGL2RenderingContext, type: number, source: string): WebGLShader {
  const shader = gl.createShader(type);
  if (!shader) throw new Error('no shader');
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) throw new Error(gl.getShaderInfoLog(shader) ?? 'shader did not compile');
  return shader;
}

function createProgram(gl: WebGL2RenderingContext): Program {
  const program = gl.createProgram();
  if (!program) throw new Error('no program');
  gl.attachShader(program, compile(gl, gl.VERTEX_SHADER, VERTEX));
  gl.attachShader(program, compile(gl, gl.FRAGMENT_SHADER, FRAGMENT));
  gl.linkProgram(program);
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) throw new Error(gl.getProgramInfoLog(program) ?? 'program did not link');

  // Its own vertex array, so the map's own drawing state is left as it was.
  const vao = gl.createVertexArray();
  const buffer = gl.createBuffer();
  if (!vao || !buffer) throw new Error('no vertex buffer');
  gl.bindVertexArray(vao);
  gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
  gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([0, 0, 1, 0, 0, 1, 1, 1]), gl.STATIC_DRAW);
  const position = gl.getAttribLocation(program, 'a_pos');
  gl.enableVertexAttribArray(position);
  gl.vertexAttribPointer(position, 2, gl.FLOAT, false, 0, 0);
  gl.bindVertexArray(null);

  const palette = gl.createTexture();
  if (!palette) throw new Error('no palette texture');
  gl.bindTexture(gl.TEXTURE_2D, palette);
  gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
  gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
  gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, false);
  gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA8, 256, 2, 0, gl.RGBA, gl.UNSIGNED_BYTE, paletteTexture());
  setSampling(gl);

  const uniforms = Object.fromEntries(UNIFORMS.map((name) => [name, gl.getUniformLocation(program, name)])) as Program['uniforms'];
  return { program, vao, buffer, palette, uniforms };
}

/** Smooth reads that stop at the edge. */
function setSampling(gl: WebGL2RenderingContext): void {
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
}

function upload(gl: WebGL2RenderingContext, entry: Entry): WebGLTexture | null {
  const texture = gl.createTexture();
  if (!texture) return null;
  gl.bindTexture(gl.TEXTURE_2D, texture);
  gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
  gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
  gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, false);
  gl.texImage2D(gl.TEXTURE_2D, 0, gl.RG8, entry.width, entry.height, 0, gl.RG, gl.UNSIGNED_BYTE, entry.field);
  setSampling(gl);
  return texture;
}

/** The map's matrix for Mercator units, narrowed to a box drawn from (0, 0) to (1, 1); worked out here, in full precision. */
export function boxMatrix(m: ArrayLike<number>, box: Box): Float32Array {
  const out = new Float32Array(16);
  for (let r = 0; r < 4; r += 1) {
    out[r] = m[r] * box.width;
    out[4 + r] = m[4 + r] * box.height;
    out[8 + r] = m[8 + r];
    out[12 + r] = m[r] * box.x + m[4 + r] * box.y + m[12 + r];
  }
  return out;
}

/** The smallest box around both. */
export function unionBox(a: Box, b: Box | null): Box {
  if (!b) return a;
  const x = Math.min(a.x, b.x);
  const y = Math.min(a.y, b.y);
  return { x, y, width: Math.max(a.x + a.width, b.x + b.width) - x, height: Math.max(a.y + a.height, b.y + b.height) - y };
}

/** Where a field sits within the drawn box: field position = box position * xy + zw. */
export function placement(field: Box, box: Box): [number, number, number, number] {
  return [box.width / field.width, box.height / field.height, (box.x - field.x) / field.width, (box.y - field.y) / field.height];
}

export class RainLayer implements CustomLayerInterface {
  readonly type = 'custom';
  readonly renderingMode = '2d';
  private map: MapLibreMap | null = null;
  private gl: WebGL2RenderingContext | null = null;
  private program: Program | null = null;
  private broken = false;
  private readonly entries = new Map<string, Entry>();
  /** The field on screen, the one it is moving from, and one fading out with nothing after it. */
  private nowKey: string | null = null;
  private beforeKey: string | null = null;
  private leavingKey: string | null = null;
  private fadeStart = 0;
  private opacity = 1;

  constructor(
    readonly id: string,
    private fadeMs: number,
  ) {}

  setFadeMs(ms: number): void {
    this.fadeMs = ms;
  }

  /** Shows a field: at once if it is the one on screen, else moving to it from the one before. */
  show(key: string, geometry: FieldGeometry, field: RainField, opacity: number): void {
    this.opacity = opacity;
    const known = this.entries.get(key);
    if (known) {
      // Most recently used goes last.
      this.entries.delete(key);
      this.entries.set(key, known);
    } else {
      this.entries.set(key, { box: mercatorBox(geometry), width: geometry.width, height: geometry.height, field, texture: null });
    }
    if (this.nowKey !== key) {
      this.beforeKey = this.fadeMs > 0 ? (this.nowKey ?? this.leavingKey) : null;
      this.nowKey = key;
      this.leavingKey = null;
      this.fadeStart = performance.now();
    }
    this.evict();
    this.map?.triggerRepaint();
  }

  /** Nothing to show for now: what is on screen fades out. */
  clear(): void {
    if (this.nowKey === null) return;
    this.leavingKey = this.fadeMs > 0 ? this.nowKey : null;
    this.nowKey = null;
    this.beforeKey = null;
    this.fadeStart = performance.now();
    this.map?.triggerRepaint();
  }

  onAdd(map: MapLibreMap, gl: WebGL2RenderingContext): void {
    this.map = map;
    this.gl = gl;
    map.on('webglcontextlost', this.onContextLost);
  }

  onRemove(map: MapLibreMap, gl: WebGL2RenderingContext): void {
    map.off('webglcontextlost', this.onContextLost);
    for (const entry of this.entries.values()) if (entry.texture) gl.deleteTexture(entry.texture);
    if (this.program) {
      gl.deleteProgram(this.program.program);
      gl.deleteVertexArray(this.program.vao);
      gl.deleteBuffer(this.program.buffer);
      gl.deleteTexture(this.program.palette);
    }
    this.entries.clear();
    this.program = null;
    this.map = null;
    this.gl = null;
  }

  render(gl: WebGL2RenderingContext, options: CustomRenderMethodInput): void {
    const t = this.fadeMs > 0 ? Math.min(1, (performance.now() - this.fadeStart) / this.fadeMs) : 1;
    if (t >= 1) {
      this.beforeKey = null;
      this.leavingKey = null;
    }
    const leaving = this.nowKey === null && this.leavingKey !== null;
    const now = this.entries.get(leaving ? (this.leavingKey ?? '') : (this.nowKey ?? ''));
    if (!now || this.broken) return;
    const before = !leaving && this.beforeKey !== null ? (this.entries.get(this.beforeKey) ?? null) : null;

    if (!this.program) {
      try {
        this.program = createProgram(gl);
      } catch (err) {
        // Without its program the layer stays empty rather than break the map's drawing.
        console.warn('[map] the rain layer cannot be drawn on this device', err);
        this.broken = true;
        return;
      }
    }
    const p = this.program;
    for (const entry of [now, before]) if (entry && !entry.texture) entry.texture = upload(gl, entry);
    if (!now.texture || (before && !before.texture)) return;

    const box = unionBox(now.box, before?.box ?? null);
    const other = before ?? now;
    gl.useProgram(p.program);
    gl.bindVertexArray(p.vao);
    gl.uniformMatrix4fv(p.uniforms.u_matrix, false, boxMatrix(options.defaultProjectionData.mainMatrix, box));
    gl.uniform4fv(p.uniforms.u_nowMap, placement(now.box, box));
    gl.uniform4fv(p.uniforms.u_beforeMap, placement(other.box, box));
    gl.uniform2f(p.uniforms.u_nowSize, now.width, now.height);
    gl.uniform2f(p.uniforms.u_beforeSize, other.width, other.height);
    gl.uniform1f(p.uniforms.u_mix, before ? t : 1);
    gl.uniform1f(p.uniforms.u_opacity, leaving ? this.opacity * (1 - t) : this.opacity);
    this.bind(gl, 0, now.texture, p.uniforms.u_now);
    this.bind(gl, 1, other.texture ?? now.texture, p.uniforms.u_before);
    this.bind(gl, 2, p.palette, p.uniforms.u_palette);
    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    gl.bindVertexArray(null);

    if (t < 1) this.map?.triggerRepaint();
  }

  private bind(gl: WebGL2RenderingContext, unit: number, texture: WebGLTexture, uniform: WebGLUniformLocation | null): void {
    gl.activeTexture(gl.TEXTURE0 + unit);
    gl.bindTexture(gl.TEXTURE_2D, texture);
    gl.uniform1i(uniform, unit);
  }

  /** Least recently shown fields go first; the ones on screen stay. */
  private evict(): void {
    for (const [key, entry] of this.entries) {
      if (this.entries.size <= KEPT) break;
      if (key === this.nowKey || key === this.beforeKey || key === this.leavingKey) continue;
      if (entry.texture && this.gl && !this.gl.isContextLost()) this.gl.deleteTexture(entry.texture);
      this.entries.delete(key);
    }
  }

  /** A lost context takes every texture with it; they are uploaded again from the fields kept here. */
  private readonly onContextLost = (): void => {
    this.program = null;
    for (const entry of this.entries.values()) entry.texture = null;
  };
}
