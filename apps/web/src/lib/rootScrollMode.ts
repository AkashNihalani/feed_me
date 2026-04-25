'use client';

type RootScrollSnapshot = {
  htmlOverflow: string;
  htmlHeight: string;
  bodyOverflow: string;
  bodyHeight: string;
  mainOverflow: string;
  mainHeight: string;
};

const classLeases = new Map<string, number>();
let rootScrollLeaseCount = 0;
let rootScrollSnapshot: RootScrollSnapshot | null = null;

function getMainElement(): HTMLElement | null {
  if (typeof document === 'undefined') return null;
  const main = document.querySelector('main');
  return main instanceof HTMLElement ? main : null;
}

function applyRootPageScroll() {
  const html = document.documentElement;
  const body = document.body;
  const main = getMainElement();

  html.style.overflow = 'auto';
  html.style.height = 'auto';
  body.style.overflow = 'auto';
  body.style.height = 'auto';
  if (main) {
    main.style.overflow = 'visible';
    main.style.height = 'auto';
  }
}

function restoreRootScroll(snapshot: RootScrollSnapshot) {
  const html = document.documentElement;
  const body = document.body;
  const main = getMainElement();

  html.style.overflow = snapshot.htmlOverflow;
  html.style.height = snapshot.htmlHeight;
  body.style.overflow = snapshot.bodyOverflow;
  body.style.height = snapshot.bodyHeight;
  if (main) {
    main.style.overflow = snapshot.mainOverflow;
    main.style.height = snapshot.mainHeight;
  }
}

export function acquireRootPageScroll(): () => void {
  if (typeof document === 'undefined') return () => {};

  if (rootScrollLeaseCount === 0) {
    const html = document.documentElement;
    const body = document.body;
    const main = getMainElement();
    rootScrollSnapshot = {
      htmlOverflow: html.style.overflow,
      htmlHeight: html.style.height,
      bodyOverflow: body.style.overflow,
      bodyHeight: body.style.height,
      mainOverflow: main?.style.overflow ?? '',
      mainHeight: main?.style.height ?? '',
    };
  }

  rootScrollLeaseCount += 1;
  applyRootPageScroll();

  let released = false;
  return () => {
    if (released || typeof document === 'undefined') return;
    released = true;

    rootScrollLeaseCount = Math.max(0, rootScrollLeaseCount - 1);
    if (rootScrollLeaseCount === 0) {
      if (rootScrollSnapshot) restoreRootScroll(rootScrollSnapshot);
      rootScrollSnapshot = null;
      return;
    }

    applyRootPageScroll();
  };
}

export function acquireDocumentClass(className: string): () => void {
  if (typeof document === 'undefined') return () => {};

  const count = classLeases.get(className) ?? 0;
  classLeases.set(className, count + 1);
  document.documentElement.classList.add(className);

  let released = false;
  return () => {
    if (released || typeof document === 'undefined') return;
    released = true;

    const nextCount = Math.max(0, (classLeases.get(className) ?? 0) - 1);
    if (nextCount === 0) {
      classLeases.delete(className);
      document.documentElement.classList.remove(className);
      return;
    }

    classLeases.set(className, nextCount);
  };
}
