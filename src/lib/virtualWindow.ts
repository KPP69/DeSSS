/**
 * Pure windowing math for DataTable virtual scrolling (F4).
 *
 * The table renders only the rows around the viewport plus an
 * overscan margin; spacer rows above and below reserve the space of
 * the rows that are not in the DOM. Heights come from measurement
 * when available and fall back to the caller's estimate, so a row
 * whose height was never measured still gets a sane slot.
 */
export type WindowResult = {
  /** First rendered row index (inclusive). */
  start: number;
  /** One past the last rendered row index (exclusive). */
  end: number;
  /** Height reserved above the rendered window. */
  offsetBefore: number;
  /** Height reserved below the rendered window. */
  offsetAfter: number;
  /** Total height of all rows. */
  totalHeight: number;
};

export function computeWindow(
  count: number,
  heightAt: (i: number) => number,
  scrollTop: number,
  viewportHeight: number,
  overscan: number,
): WindowResult {
  if (count <= 0) {
    return { start: 0, end: 0, offsetBefore: 0, offsetAfter: 0, totalHeight: 0 };
  }
  const heights: number[] = new Array(count);
  let totalHeight = 0;
  for (let i = 0; i < count; i++) {
    heights[i] = Math.max(1, heightAt(i));
    totalHeight += heights[i];
  }
  // First row whose bottom edge is below the viewport top.
  let start = 0;
  let acc = 0;
  while (start < count && acc + heights[start] <= scrollTop) {
    acc += heights[start];
    start++;
  }
  // First row whose top edge is at or below the viewport bottom.
  let end = start;
  let accEnd = acc;
  const bottom = scrollTop + viewportHeight;
  while (end < count && accEnd < bottom) {
    accEnd += heights[end];
    end++;
  }
  const visStart = Math.max(0, start - overscan);
  const visEnd = Math.min(count, end + overscan);
  let offsetBefore = 0;
  for (let i = 0; i < visStart; i++) offsetBefore += heights[i];
  let visibleHeight = 0;
  for (let i = visStart; i < visEnd; i++) visibleHeight += heights[i];
  return {
    start: visStart,
    end: visEnd,
    offsetBefore,
    offsetAfter: totalHeight - offsetBefore - visibleHeight,
    totalHeight,
  };
}
