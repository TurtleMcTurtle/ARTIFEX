export type TaskRetentionSettings = {
  enabled: boolean;
  archive_after_days: number;
  delete_after_archive_days: number;
  check_interval_hours: number;
  batch_size: number;
  cycle_timeout_seconds: number;
  source: "environment" | "saved";
  revision: string;
  scheduler_available: boolean;
  scheduler_running: boolean;
  next_check_unix: number | null;
};

export type TaskRetentionInput = Pick<TaskRetentionSettings,
  "enabled" | "archive_after_days" | "delete_after_archive_days" | "check_interval_hours" | "batch_size" | "cycle_timeout_seconds" | "revision"
> & { confirm_enable: boolean };

export type TaskRetentionForm = {
  enabled: boolean;
  archiveDays: string;
  deleteDays: string;
  intervalHours: string;
  batchSize: string;
  timeoutSeconds: string;
};

export function retentionForm(settings: TaskRetentionSettings): TaskRetentionForm {
  return {
    enabled: settings.enabled,
    archiveDays: String(settings.archive_after_days),
    deleteDays: String(settings.delete_after_archive_days),
    intervalHours: String(settings.check_interval_hours),
    batchSize: String(settings.batch_size),
    timeoutSeconds: String(settings.cycle_timeout_seconds),
  };
}

export function retentionInput(form: TaskRetentionForm, revision: string, confirm: boolean): TaskRetentionInput | null {
  function integer(raw: string, max: number) {
    if (!/^\d+$/.test(raw)) return null;
    const value = Number(raw);
    return Number.isSafeInteger(value) && value >= 1 && value <= max ? value : null;
  }
  const archive = integer(form.archiveDays, 3650);
  const residence = integer(form.deleteDays, 3650);
  const interval = integer(form.intervalHours, 168);
  const batch = integer(form.batchSize, 100);
  const timeout = integer(form.timeoutSeconds, 600);
  if (archive === null || residence === null || interval === null || batch === null || timeout === null || !revision) return null;
  return {
    enabled: form.enabled,
    archive_after_days: archive,
    delete_after_archive_days: residence,
    check_interval_hours: interval,
    batch_size: batch,
    cycle_timeout_seconds: timeout,
    revision,
    confirm_enable: confirm,
  };
}

export function checkedRetentionSettings(value: TaskRetentionSettings): TaskRetentionSettings {
  const values = [value?.archive_after_days, value?.delete_after_archive_days, value?.check_interval_hours,
    value?.batch_size, value?.cycle_timeout_seconds];
  if (!value || typeof value.enabled !== "boolean" || typeof value.revision !== "string" || !value.revision ||
    !values.every((number) => typeof number === "number" && Number.isFinite(number) && number > 0) ||
    (value.source !== "saved" && value.source !== "environment") || typeof value.scheduler_available !== "boolean" ||
    typeof value.scheduler_running !== "boolean" || (value.next_check_unix !== null &&
      (!Number.isSafeInteger(value.next_check_unix) || value.next_check_unix <= 0))) {
    throw new Error("Invalid retention settings response");
  }
  return value;
}
