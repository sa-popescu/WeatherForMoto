import { useEffect, useRef, useState } from 'react';

// A file dragged anywhere over the window while "Traseu" is on screen: the
// screen shows where to drop it, and the first file dropped goes to `onFile`
// (the same GPX import as the button). Without this, a file dropped on the
// page makes the browser leave the app to open it.

function carriesFiles(event: DragEvent): boolean {
  return Array.from(event.dataTransfer?.types ?? []).includes('Files');
}

/** True while a file is being dragged over the window. */
export function useFileDrop(active: boolean, onFile: (file: File) => void): boolean {
  const [over, setOver] = useState(false);
  const onFileRef = useRef(onFile);
  onFileRef.current = onFile;

  useEffect(() => {
    if (!active) {
      setOver(false);
      return undefined;
    }
    // dragenter and dragleave fire for every element crossed; count them so
    // moving over the page's own children does not make the overlay flicker.
    let depth = 0;
    const enter = (event: DragEvent): void => {
      if (!carriesFiles(event)) return;
      event.preventDefault();
      depth += 1;
      setOver(true);
    };
    const over = (event: DragEvent): void => {
      if (!carriesFiles(event)) return;
      event.preventDefault();
      if (event.dataTransfer) event.dataTransfer.dropEffect = 'copy';
    };
    const leave = (event: DragEvent): void => {
      if (!carriesFiles(event)) return;
      depth = Math.max(0, depth - 1);
      if (depth === 0) setOver(false);
    };
    const drop = (event: DragEvent): void => {
      if (!carriesFiles(event)) return;
      event.preventDefault();
      depth = 0;
      setOver(false);
      const file = event.dataTransfer?.files[0];
      if (file) onFileRef.current(file);
    };
    window.addEventListener('dragenter', enter);
    window.addEventListener('dragover', over);
    window.addEventListener('dragleave', leave);
    window.addEventListener('drop', drop);
    return () => {
      window.removeEventListener('dragenter', enter);
      window.removeEventListener('dragover', over);
      window.removeEventListener('dragleave', leave);
      window.removeEventListener('drop', drop);
    };
  }, [active]);

  return over;
}
