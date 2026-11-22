import React, { useEffect, useMemo, useState } from "react";
import { EXCHANGES, activeFields, getExchange, validateField } from "../lib/exchanges";
import type { AccountView, CredentialMap } from "../lib/types";
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
 * Security notes (unchanged from phase 2):
 * - Secret inputs live only in this component's local state and are wiped
 *   on close, cancel and successful submit. They are never written to a
 *   store, localStorage or the console.
 * - In edit mode the exchange selector is disabled (greyed out) and the
 *   backend rejects exchange changes as well.
 * - In edit mode, leaving every credential field empty keeps the stored
 *   credentials; filling any field requires the full set for that exchange.
 */
export default function AccountModal({ open, editing, onClose }: Props) {
  const create = useAccountStore((s) => s.create);
  const update = useAccountStore((s) => s.update);
  const isEdit = editing !== null;

  const [label, setLabel] = useState("");
  const [exchangeId, setExchangeId] = useState(EXCHANGES[0].id);
  const [mode, setMode] = useState("api");
  const [values, setValues] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const def = useMemo(() => getExchange(exchangeId), [exchangeId]);
  const fields = useMemo(() => activeFields(def, mode), [def, mode]);

  function reset() {
    setLabel("");
    setExchangeId(EXCHANGES[0].id);
    setMode("api");
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
    setMode("api");
  }

  async function submit() {
    setFormError(null);
    if (label.trim() === "") {
      setFormError("请填写账户名");
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
        credentials[f.key] = f.type === "number" ? Number(raw) : raw;
      }
      if (def.modes) credentials["mode"] = mode;
    }

    setSubmitting(true);
    const ok = isEdit
      ? await update(editing.id, label.trim(), exchangeId, credentials)
      : await create(label.trim(), exchangeId, credentials ?? {});
    setSubmitting(false);
    if (ok) {
      close();
    } else {
      setFormError(useAccountStore.getState().error ?? "保存失败");
    }
  }

  const inputCls =
    "w-full bg-panel border border-line rounded px-3 py-2 text-sm text-t1 outline-none focus:border-primary";
  const labelCls = "block text-xs text-t2 mb-1";

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
          <Button onClick={submit} disabled={submitting}>
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
              options={EXCHANGES.map((e) => ({ value: e.id, label: e.name }))}
              value={exchangeId}
              onChange={pickExchange}
              disabled={isEdit}
            />
            {isEdit ? (
              <p className="text-xs text-t3 mt-1">
                交易所创建后不可修改；如需更换，请删除后重新创建。
              </p>
            ) : (
              def.note && <p className="text-xs text-t3 mt-1">{def.note}</p>
            )}
          </div>
          {isEdit && (
            <div className="text-xs text-t3 space-y-1">
              <p>
                标识：<span className="num text-t2">{editing.maskedIdentifier}</span>
              </p>
              <p>
                凭证状态：
                <span className={editing.credentialConfigured ? "text-profit" : "text-warn"}>
                  {editing.credentialConfigured ? "已配置" : "未配置"}
                </span>
              </p>
              <p>凭证输入框留空表示保留原凭证，填写则需填写完整并整体替换。</p>
            </div>
          )}
        </div>

        {/* Right column: per-exchange credential form */}
        <div className="space-y-4 min-w-0">
          {def.modes && (
            <div>
              <label className={labelCls}>接入方式</label>
              <div className="flex gap-2">
                {def.modes.map((m) => (
                  <button
                    key={m.id}
                    type="button"
                    onClick={() => {
                      setMode(m.id);
                      setValues({});
                    }}
                    className={`px-3 py-1.5 text-sm rounded border ${
                      mode === m.id
                        ? "border-primary text-primary bg-panel"
                        : "border-line text-t2 bg-panel/50"
                    }`}
                  >
                    {m.label}
                  </button>
                ))}
              </div>
            </div>
          )}
          {fields.map((f) => (
            <div key={f.key}>
              <label className={labelCls}>
                {f.label}
                {f.required && <span className="text-loss"> *</span>}
              </label>
              <input
                className={inputCls}
                type={f.type === "password" ? "password" : f.type === "number" ? "number" : "text"}
                value={values[f.key] ?? ""}
                autoComplete="off"
                onChange={(e) => setValues((v) => ({ ...v, [f.key]: e.target.value }))}
                placeholder={isEdit ? "留空保留原凭证" : undefined}
              />
              {f.hint && <p className="text-xs text-warn/80 mt-1">{f.hint}</p>}
            </div>
          ))}
          <p className="text-xs text-t3">
            凭证只会加密保存在本机 ./data/vault.enc，不会上传或写入数据库明文。
          </p>
        </div>
      </div>

      {formError && (
        <div className="mt-4 text-sm text-loss bg-loss/10 border border-loss/30 rounded px-3 py-2">
          {formError}
        </div>
      )}
    </Modal>
  );
}
