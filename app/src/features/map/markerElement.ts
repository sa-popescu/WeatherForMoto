// A marker's DOM element: a box of a fixed size, whose children (map-markers.css)
// are placed inside it. The HTML is always a constant of the app, never text
// from a report or a place name.

export function markerElement(className: string, html: string, width: number, height: number): HTMLElement {
  const el = document.createElement('div');
  el.className = className;
  el.style.width = `${width}px`;
  el.style.height = `${height}px`;
  el.innerHTML = html;
  return el;
}
