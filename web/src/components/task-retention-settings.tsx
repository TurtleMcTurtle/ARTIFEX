"use client";

import * as React from "react";
import { ArchiveIcon, Loader2Icon } from "lucide-react";
import { toast } from "sonner";
import { useI18n } from "@/i18n";
import { api } from "@/lib/api";
import { checkedRetentionSettings, retentionForm, retentionInput, type TaskRetentionForm, type TaskRetentionSettings as RetentionSettings } from "@/lib/task-retention";
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle } from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";

export function TaskRetentionSettings() {
  "use no memo";
  const { t } = useI18n();
  const [open, setOpen] = React.useState(false);
  const [loaded, setLoaded] = React.useState<RetentionSettings | null>(null);
  const [form, setForm] = React.useState<TaskRetentionForm | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [saving, setSaving] = React.useState(false);
  const [error, setError] = React.useState("");
  const [reloadRequired, setReloadRequired] = React.useState(false);
  const [refresh, setRefresh] = React.useState(0);
  const [confirmOpen, setConfirmOpen] = React.useState(false);
  const savingRef = React.useRef(false);

  React.useEffect(() => {
    if (!open) return;
    let active = true;
    setLoading(true);
    setLoaded(null);
    setForm(null);
    setError("");
    setReloadRequired(false);
    api.taskRetention().then(checkedRetentionSettings).then((settings) => {
      if (!active) return;
      setLoaded(settings);
      setForm(retentionForm(settings));
    }).catch((e) => {
      if (active) setError(t("settings.retentionLoadError") + " " + (e as Error).message);
    }).finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [open, refresh, t]);

  function change(field: keyof TaskRetentionForm, value: string | boolean) {
    setForm((current) => current ? { ...current, [field]: value } : current);
    setError("");
  }

  async function persist(confirm: boolean) {
    if (!form || !loaded || loading || savingRef.current || reloadRequired) return;
    const input = retentionInput(form, loaded.revision, confirm);
    if (!input) { setError(t("settings.retentionInvalid")); return; }
    savingRef.current = true;
    setSaving(true);
    setError("");
    try {
      const result = checkedRetentionSettings(await api.saveTaskRetention(input));
      setLoaded(result);
      setForm(retentionForm(result));
      setConfirmOpen(false);
      toast.success(t("settings.retentionSaved"));
      setOpen(false);
    } catch (e) {
      setConfirmOpen(false);
      setError(t("settings.retentionSaveError") + " " + (e as Error).message);
      setReloadRequired(true);
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  }

  async function save() {
    if (!form || !loaded || loading || saving || reloadRequired) return;
    if (!retentionInput(form, loaded.revision, false)) { setError(t("settings.retentionInvalid")); return; }
    if (form.enabled) { setConfirmOpen(true); return; }
    await persist(false);
  }

  const disabled = loading || saving || !form || reloadRequired;
  const total = form ? Number(form.archiveDays) + Number(form.deleteDays) : 0;
  const fields = [
    { id: "retention-archive-days", key: "archiveDays" as const, label: t("settings.retentionArchiveDays"), help: t("settings.retentionArchiveHelp"), max: 3650 },
    { id: "retention-delete-days", key: "deleteDays" as const, label: t("settings.retentionDeleteDays"), help: t("settings.retentionDeleteHelp"), max: 3650 },
    { id: "retention-interval-hours", key: "intervalHours" as const, label: t("settings.retentionInterval"), help: t("settings.retentionIntervalHelp"), max: 168 },
  ];

  return (
    <>
      <Dialog open={open} onOpenChange={(next) => { if (!savingRef.current) { setConfirmOpen(false); setOpen(next); } }}>
        <DialogTrigger asChild>
          <Button size="sm" variant="outline" aria-label={t("settings.retentionTitle")}>
            <ArchiveIcon /> {t("settings.retentionTitle")}
          </Button>
        </DialogTrigger>
        <DialogContent className="flex max-h-[90dvh] flex-col overflow-hidden sm:max-w-lg" style={{ width: "calc(100% - 2rem)" }}>
          <DialogHeader className="shrink-0 pr-6">
            <DialogTitle>{t("settings.retentionTitle")}</DialogTitle>
            <DialogDescription>{t("settings.retentionDescription")}</DialogDescription>
          </DialogHeader>
          <div className="min-h-0 overflow-y-auto">
          {loading && <p className="flex items-center gap-2 text-sm"><Loader2Icon className="size-4 animate-spin" />{t("settings.retentionLoading")}</p>}
          {form && loaded && (
            <div className="grid gap-4 py-2">
              <div className="flex items-center justify-between gap-4">
                <Label htmlFor="retention-enabled">{t("settings.retentionEnabled")}</Label>
                <Switch id="retention-enabled" checked={form.enabled} disabled={disabled} onCheckedChange={(checked) => change("enabled", checked)} />
              </div>
              {fields.map((field) => (
                <div key={field.id} className="grid gap-1.5">
                  <Label htmlFor={field.id}>{field.label}</Label>
                  <Input id={field.id} type="number" min={1} max={field.max} step={1} className="w-32" value={form[field.key]} disabled={disabled} onChange={(event) => change(field.key, event.target.value)} />
                  <p className="text-xs text-muted-foreground">{field.help}</p>
                </div>
              ))}
              {Number.isSafeInteger(total) && total > 0 && <p className="text-sm rounded-md bg-muted p-3">{t("settings.retentionSummary", { archive: form.archiveDays, residence: form.deleteDays, total })}</p>}
              <details className="text-sm">
                <summary className="cursor-pointer">{t("settings.retentionAdvanced")}</summary>
                <div className="mt-3 grid grid-cols-2 gap-3">
                  <div className="grid gap-1.5"><Label htmlFor="retention-batch">{t("settings.retentionBatch")}</Label><Input id="retention-batch" type="number" min={1} max={100} value={form.batchSize} disabled={disabled} onChange={(event) => change("batchSize", event.target.value)} /></div>
                  <div className="grid gap-1.5"><Label htmlFor="retention-timeout">{t("settings.retentionTimeout")}</Label><Input id="retention-timeout" type="number" min={1} max={600} value={form.timeoutSeconds} disabled={disabled} onChange={(event) => change("timeoutSeconds", event.target.value)} /></div>
                </div>
              </details>
              <p className="text-xs text-muted-foreground">{t(loaded.source === "saved" ? "settings.retentionSavedSource" : "settings.retentionEnvironmentSource")}</p>
              {loaded.enabled && !loaded.scheduler_available && <p className="text-sm text-amber-600">{t("settings.retentionUnavailable")}</p>}
              {loaded.enabled && loaded.scheduler_available && <p className="text-xs text-muted-foreground">{loaded.next_check_unix ? t("settings.retentionNextCheck", { time: new Date(loaded.next_check_unix * 1000).toLocaleString() }) : t("settings.retentionApplying")}</p>}
              <p className="text-xs text-muted-foreground">{t("settings.retentionQueuedWarning")}</p>
            </div>
          )}
          {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
          </div>
          <DialogFooter className="shrink-0">
            <Button id="retention-reload" variant="ghost" disabled={loading || saving} onClick={() => setRefresh((value) => value + 1)}>{t("settings.retentionReload")}</Button>
            <DialogClose asChild><Button variant="outline" disabled={saving}>{t("settings.retentionCancel")}</Button></DialogClose>
            <Button id="retention-save" onClick={save} disabled={disabled || !loaded}>{saving && <Loader2Icon className="size-4 animate-spin" />}{t("settings.retentionSave")}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <AlertDialog open={confirmOpen} onOpenChange={(next) => { if (!saving) setConfirmOpen(next); }}>
        <AlertDialogContent>
          <AlertDialogHeader><AlertDialogTitle>{t("settings.retentionConfirmTitle")}</AlertDialogTitle><AlertDialogDescription>{t("settings.retentionConfirmWarning")}</AlertDialogDescription></AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel id="retention-confirm-cancel" disabled={saving} onClick={() => setConfirmOpen(false)}>{t("settings.retentionCancel")}</AlertDialogCancel>
            <AlertDialogAction id="retention-confirm-enable" disabled={saving} onClick={(event) => { event.preventDefault(); void persist(true); }}>{saving && <Loader2Icon className="size-4 animate-spin" />}{t("settings.retentionConfirmSave")}</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}
