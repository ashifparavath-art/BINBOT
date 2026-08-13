/**
 * "Today" is deliberately computed in the viewer's own browser timezone,
 * not the server's (UTC) — the backend backfills a generous rolling
 * window (see server/dashboard_bridge.py's DailyJsonlLog usage) and this
 * is where that window gets narrowed down to what actually counts as
 * "today" on screen, matching every other timestamp already rendered via
 * formatTime()'s toLocaleTimeString().
 */
export function isLocalToday(epochMs: number): boolean {
  return new Date(epochMs).toDateString() === new Date().toDateString();
}
