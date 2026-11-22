import React, { useEffect, useMemo, useState } from "react";
import { EXCHANGES, activeFields, getExchange, validateField } from "../lib/exchanges";
import type { AccountView, CredentialMap } from "../lib/types";
import { useAccountStore } from "../store/useAccountStore";

type Props = {
  open: boolean;
  /** null = add mode, otherwise the account being edited. */
  editing: AccountView | null;
  onClose: () => void;
};

/**
 * Add / edit account modal.
 *
 * Security notes:
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
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const def = useMemo(() => getExchange(exchangeId), [exchangeId]);
  const fields = useMemo(() => activeFields(def, mode), [def, mode]);

  function reset() {
    setLabel("");
    setExchangeId(EXCHANGES[0].id);
    setMode("api");
    setValues({});
    setDropdownOpen(false);
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

  if (!open) return null;

  function close() {
    reset(); // wipe any typed secrets before unmounting
    onClose();
  }

  function pickExchange(id: string) {
    if (isEdit) return; // locked in edit mode
    setExchangeId(id);
    setValues({}); // never reuse field values across exchanges
    setMode("api");
    setDropdownOpen(false);
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
    "w-full bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm outline-none focus:border-sky-600";
  const labelCls = "block text-xs text-slate-400 mb-1";

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-slate-900 border border-slate-700 rounded-lg w-[760px] max-w-[94vw] max-h-[90vh] overflow-y-auto p-6">
        <div className="flex items-center justify-between mb-5">
          <h2 className="text-lg font-semibold">{isEdit ? "编辑账户" : "添加账户"}</h2>
          <button onClick={close} className="text-slate-400 hover:text-slate-200 text-xl leading-none">
            ×
          </button>
        </div>

        <div className="grid grid-cols-2 gap-6">
          {/* Left column: label + exchange */}
          <div className="space-y-4">
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
              <div className="relative">
                <button
                  type="button"
                  disabled={isEdit}
                  onClick={() => setDropdownOpen((v) => !v)}
                  className={`w-full text-left px-3 py-2 text-sm rounded border ${
                    isEdit
                      ? "bg-slate-800/50 border-slate-800 text-slate-500 cursor-not-allowed"
                      : "bg-slate-800 border-slate-700 hover:border-slate-500"
                  }`}
                >
                  {def.name}
                  {!isEdit && <span className="float-right text-slate-500">▾</span>}
                </button>
                {dropdownOpen && !isEdit && (
                  <div className="absolute z-10 mt-1 w-full max-h-60 overflow-y-auto bg-slate-800 border border-slate-600 rounded shadow-xl">
                    {EXCHANGES.map((e) => (
                      <button
                        key={e.id}
                        type="button"
                        onClick={() => pickExchange(e.id)}
                        className={`block w-full text-left px-3 py-2 text-sm hover:bg-slate-700 ${
                          e.id === exchangeId ? "text-sky-400" : ""
                        }`}
                      >
                        {e.name}
                      </button>
                    ))}
                  </div>
                )}
              </div>
              {isEdit ? (
                <p className="text-xs text-slate-500 mt-1">
                  交易所创建后不可修改；如需更换，请删除后重新创建。
                </p>
              ) : (
                def.note && <p className="text-xs text-slate-500 mt-1">{def.note}</p>
              )}
            </div>
            {isEdit && (
              <div className="text-xs text-slate-500 space-y-1">
                <p>
                  标识：<span className="font-mono text-slate-400">{editing.maskedIdentifier}</span>
                </p>
                <p>
                  凭证状态：
                  <span className={editing.credentialConfigured ? "text-emerald-400" : "text-amber-400"}>
                    {editing.credentialConfigured ? "已配置" : "未配置"}
                  </span>
                </p>
                <p>凭证输入框留空表示保留原凭证，填写则需填写完整并整体替换。</p>
              </div>
            )}
          </div>

          {/* Right column: per-exchange credential form */}
          <div className="space-y-4">
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
                          ? "border-sky-600 text-sky-400 bg-slate-800"
                          : "border-slate-700 text-slate-400 bg-slate-800/50"
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
                  {f.required && <span className="text-rose-400"> *</span>}
                </label>
                <input
                  className={inputCls}
                  type={f.type === "password" ? "password" : f.type === "number" ? "number" : "text"}
                  value={values[f.key] ?? ""}
                  autoComplete="off"
                  onChange={(e) => setValues((v) => ({ ...v, [f.key]: e.target.value }))}
                  placeholder={isEdit ? "留空保留原凭证" : undefined}
                />
                {f.hint && <p className="text-xs text-amber-400/80 mt-1">{f.hint}</p>}
              </div>
            ))}
            <p className="text-xs text-slate-600">
              凭证只会加密保存在本机 ./data/vault.enc，不会上传或写入数据库明文。
            </p>
          </div>
        </div>

        {formError && (
          <div className="mt-4 text-sm text-rose-400 bg-rose-950/40 border border-rose-900 rounded px-3 py-2">
            {formError}
          </div>
        )}

        <div className="flex justify-end gap-3 mt-6">
          <button
            onClick={close}
            className="px-4 py-2 text-sm rounded border border-slate-600 text-slate-300 hover:bg-slate-800"
          >
            取消
          </button>
          <button
            onClick={submit}
            disabled={submitting}
            className="px-4 py-2 text-sm rounded bg-sky-600 hover:bg-sky-500 disabled:opacity-50 text-white"
          >
            {submitting ? "保存中…" : isEdit ? "保存修改" : "添加账户"}
          </button>
        </div>
      </div>
    </div>
  );
}
