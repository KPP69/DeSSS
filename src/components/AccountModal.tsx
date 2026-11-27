import React, { useEffect, useMemo, useState } from "react";
import { useVenueSchemas } from "../hooks/useVenueSchemas";
import type { AccountView, CredentialMap, VenueSchemaField } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";
import { Button, Modal, SelectDropdown } from "./ui";

type Props = {
  open: boolean;
  /** null = add mode, otherwise the account being edited. */
  editing: AccountView | null;
  onClose: () => void;
};

/**
 * Add / edit account modal, built on the shared Modal + SelectDropdown.
 *
 * The form is rendered entirely from the backend venue schemas
 * (Phase 2, I10): field order, required flag, number bounds,
 * forbidden values and hints all come from `get_venue_schemas`.
 * There is no per-exchange table left on the frontend — a field
 * added in Rust shows up here with no frontend change. Validation
 * is the frontend's basic intercept only (required + number bounds);
 * the backend re-checks everything.
 *
 * Security notes (unchanged from phase 2):
 * - Secret inputs live only in this component's local state and are wiped
 *   on close, cancel and successful submit. They are never written to a
 *   store, localStorage or the console.
 * - In edit mode the exchange selector is disabled (greyed out) and the
 *   backend rejects exchange changes as well.
 * - In edit mode, leaving every credential field empty keeps the stored
 *   credentials; filling any field requires the full set for that exchange.
 */
function validateField(field: VenueSchemaField, raw: string): string | null {
  const value = raw.trim();
  if (field.required && value === "") return `请填写${field.label}`;
  if (value === "") return null;
  if (field.kind === "number") {
    const n = Number(value);
    if (!Number.isFinite(n) || !Number.isInteger(n)) return `${field.label} 必须是整数`;
    if (field.min !== undefined && n < field.min) return `${field.label} 不能小于 ${field.min}`;
    if (field.max !== undefined && n > field.max) return `${field.label} 不能大于 ${field.max}`;
    if (field.forbidden?.includes(n)) return `${field.label} 的值 ${n} 不可用`;
  }
  return null;
}

export default function AccountModal({ open, editing, onClose }: Props) {
  const create = useAccountStore((s) => s.create);
  const update = useAccountStore((s) => s.update);
  const isEdit = editing !== null;
  const { schemas, loading: schemasLoading } = useVenueSchemas();

  const [label, setLabel] = useState("");
  const [exchangeId, setExchangeId] = useState("");
  const [values, setValues] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  // The selected schema; in add mode an empty selection falls back to
  // the first schema once they arrive (schemas load once at startup).
  const def = useMemo(
    () =>
      schemas?.find((e) => e.id === exchangeId) ??
      (!isEdit ? schemas?.[0] : undefined),
    [schemas, exchangeId, isEdit],
  );
  const fields = def?.fields ?? [];

  function reset() {
    setLabel("");
    setExchangeId("");
    setValues({});
    setFormError(null);
    setSubmitting(false);
  }

  useEffect(() => {
    if (!open) return;
    reset();
    if (editing) {
      setLabel(editing.label);
      setExchangeId(editing.exchangeId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, editing?.id]);

  function close() {
    reset(); // wipe any typed secrets before unmounting
    onClose();
  }

  function pickExchange(id: string) {
    if (isEdit) return; // locked in edit mode (selector is disabled anyway)
    setExchangeId(id);
    setValues({}); // never reuse field values across exchanges
  }

  async function submit() {
    setFormError(null);
    if (label.trim() === "") {
      setFormError("请填写账户名");
      return;
    }
    if (!def) {
      setFormError("交易所信息加载中，请稍后再试");
      return;
    }
    const filled = fields.filter((f) => (values[f.key] ?? "").trim() !== "");
    let credentials: CredentialMap | null = null;

    if (!isEdit || filled.length > 0) {
      // Create always needs the full set; edit needs it once anything is typed.
      for (const f of fields) {
        const err = validateField(f, values[f.key] ?? "");
        if (err) {
          setFormError(isEdit ? `更新凭证时请填写完整：${err}` : err);
          return;
        }
      }
      credentials = {};
      for (const f of fields) {
        const raw = (values[f.key] ?? "").trim();
        credentials[f.key] = f.kind === "number" ? Number(raw) : raw;
      }
    }

    setSubmitting(true);
    const ok = isEdit
      ? await update(editing.id, label.trim(), exchangeId, credentials)
      : await create(label.trim(), def.id, credentials ?? {});
    setSubmitting(false);
    if (ok) {
      close();
    } else {
      setFormError(useAccountStore.getState().error ?? "保存失败");
    }
  }

  const inputCls =
    "w-full bg-ds-panel border border-ds-line rounded-lg px-3 h-8 text-ds-text-1 outline-none focus:border-ds-accent focus:ring-2 focus:ring-ds-focus-ring/50 disabled:bg-ds-bg-disabled disabled:text-ds-text-disabled";
  const labelCls = "block text-xs text-ds-text-2 mb-1";

  return (
    <Modal
      open={open}
      title={isEdit ? "编辑账户" : "添加账户"}
      onClose={close}
      footer={
        <>
          <Button variant="secondary" onClick={close}>
            取消
          </Button>
          <Button onClick={submit} disabled={submitting || (!def && !isEdit)}>
            {submitting ? "保存中…" : isEdit ? "保存修改" : "添加账户"}
          </Button>
        </>
      }
    >
      <div className="grid grid-cols-2 gap-6">
        {/* Left column: label + exchange (stable size, dropdown overlays) */}
        <div className="space-y-4 min-w-0">
          <div>
            <label className={labelCls}>账户名</label>
            <input
              className={inputCls}
              value={label}
              onChange={(e) => setLabel(e.target.value)}
              placeholder="例如：主账户"
            />
          </div>
          <div>
            <label className={labelCls}>交易所</label>
            <SelectDropdown
              options={(schemas ?? []).map((e) => ({ value: e.id, label: e.name }))}
              value={exchangeId || def?.id || ""}
              onChange={pickExchange}
              disabled={isEdit || schemasLoading}
            />
            {isEdit ? (
              <p className="text-xs text-ds-text-3 mt-1">
                交易所创建后不可修改；如需更换，请删除后重新创建。
              </p>
            ) : (
              def?.note && <p className="text-xs text-ds-text-3 mt-1">{def.note}</p>
            )}
          </div>
          {isEdit && (
            <div className="text-xs text-ds-text-3 space-y-1">
              <p>
                标识：<span className="num text-ds-text-2">{editing.maskedIdentifier}</span>
              </p>
              <p>
                凭证状态：
                <span className="text-ds-text-2">
                  {editing.credentialConfigured ? "已配置" : "未配置"}
                </span>
              </p>
              <p>凭证输入框留空表示保留原凭证，填写则需填写完整并整体替换。</p>
            </div>
          )}
        </div>

        {/* Right column: per-exchange credential form, schema-driven */}
        <div className="space-y-4 min-w-0">
          {!def ? (
            <p className="text-xs text-ds-text-3">交易所信息加载中…</p>
          ) : (
            fields.map((f) => (
              <div key={f.key}>
                <label className={labelCls}>
                  {f.label}
                  {f.required && <span className="text-ds-text-3"> *</span>}
                </label>
                <input
                  className={inputCls}
                  type={f.kind === "password" ? "password" : f.kind === "number" ? "number" : "text"}
                  value={values[f.key] ?? ""}
                  autoComplete="off"
                  onChange={(e) => setValues((v) => ({ ...v, [f.key]: e.target.value }))}
                  placeholder={isEdit ? "留空保留原凭证" : undefined}
                />
                {f.hint && <p className="text-xs text-ds-text-3 mt-1">{f.hint}</p>}
              </div>
            ))
          )}
          <p className="text-xs text-ds-text-3">
            凭证只会加密保存在本机 ./data/vault.enc，不会上传或写入数据库明文。
          </p>
        </div>
      </div>

      {formError && (
        <div className="mt-4 text-ds-down bg-ds-panel border border-ds-line rounded px-3 py-2">
          {formError}
        </div>
      )}
    </Modal>
  );
}
