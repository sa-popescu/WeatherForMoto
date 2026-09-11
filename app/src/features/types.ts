/** Props every tab screen receives from the shell. */
export interface ScreenProps {
  /** true while this tab is the visible one (screens stay mounted after the first visit). */
  active: boolean;
}
