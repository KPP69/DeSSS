import React, { useEffect, useState } from "react";
import { openPath } from "@tauri-apps/plugin-opener";
import { Button, Input } from "../components/ui";
import PageState from "../components/PageState";
import ThemeSwitcher from "../components/ThemeSwitcher";
import { useInvoke } from "../hooks/useInvoke";
import { api } from "../lib/tauri";
import { toast } from "../store/useToastStore";
import { useVenueStore } from "../store/useVenueStore";

/**
 * System page: four cards in two columns (one column on a narrow
 * window). Appearance holds the theme switch (the only place it
 * lives, F4.4); the proxy card saves through the backend; the data
 * / log cards show the absolute paths and open them in Explorer via
 * the opener plugin (opener:default is in the app capability). No
 * log-file count — there is no backend command for it (F5).
 *
 * Proxy + catalog both come from `useInvoke` (Phase 2, I11); saving
 * the proxy is a click mutation that refreshes the proxy afterwards.
 * The backend resolves the proxy in this order — the value set here,
 * HTTP(S)_PROXY env vars, then the Windows system proxy registry —
 * and uses it for both reqwest (REST) and the WS dial (HTTP CONNECT).
 * Applies when an account is (re)connected.
 */
const CARD = "bg-ds-panel border border-ds-line rounded-[10px] shadow-card p-4";

export default function SystemPage() {
  const proxy = useInvoke(() => api.getProxy(), []);
  const catalog = useInvoke(() => api.getCatalogStatus(), []);
  const [input, setInput] = useState("");
  const [saving, setSaving] = useState(false);
  const names = useVenueStore((s) => s.names);
  const venueName = (id: string) => names[id] ?? id;

  useEffect(() => {
    if (proxy.data) setInput(proxy.data.custom ?? "");
  }, [proxy.data]);

  const save = async () => {
    setSaving(true);
    try {
      await api.setProxy(input);
      proxy.refresh();
      toast.success("代理已保存，重新连接账户后生效");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const openDir = async (path: string) => {
    try {
      await openPath(path);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : String(e));
    }
  };

  // F4.4: no toolbar on this page (it held only the version status);
  // the theme switch lives in the 外观主题 card below.
  return (
    <div className="grid grid-cols-1 gap-5 lg:grid-cols-2">
      <div className={CARD}>
        <div className="text-base font-semibold text-ds-text-1 mb-1">外观主题</div>
        <div className="text-[13px] text-ds-text-2 mb-3 leading-relaxed">
          暖色 / 深色，切换后立即生效，主题随应用保存。
        </div>
        <ThemeSwitcher />
      </div>
      <div className={CARD}>
        <div className="text-base font-semibold text-ds-text-1 mb-1">本地代理（Clash Verge 等）</div>
        <div className="text-[13px] text-ds-text-2 mb-3 leading-relaxed">
          留空则不设置自定义代理，自动读取环境变量与 Windows 系统代理。填写后 REST 与 WS 都会走该代理。
        </div>
        <div className="flex items-center gap-2">
          <div className="flex-1 min-w-0">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="http://127.0.0.1:7897"
            />
          </div>
          <Button onClick={save} loading={saving}>
            保存
          </Button>
        </div>
        {proxy.loading && !proxy.data ? (
          <div className="mt-2">
            <PageState loading loadingText="加载代理设置…" />
          </div>
        ) : proxy.error && !proxy.data ? (
          <div className="mt-2">
            <PageState error={`代理设置加载失败：${proxy.error}`} onRetry={() => proxy.refresh()} />
          </div>
        ) : proxy.data ? (
          <div className="text-[13px] text-ds-text-3 mt-2 num">
            当前生效：{proxy.data.effective ?? "未使用代理（直连）"}
          </div>
        ) : null}
      </div>
      <div className={CARD}>
        <div className="text-base font-semibold text-ds-text-1 mb-1">数据目录</div>
        <div className="text-[13px] text-ds-text-2 mb-3 leading-relaxed">
          品种目录落库保存，搜索不走网络；后台每 12 小时刷新一次。
        </div>
        {catalog.loading && !catalog.data ? (
          <PageState loading loadingText="加载中…" />
        ) : catalog.error && !catalog.data ? (
          <PageState error={`目录状态加载失败：${catalog.error}`} onRetry={() => catalog.refresh()} />
        ) : catalog.data ? (
          <>
            <div className="text-[13px] text-ds-text-2 leading-relaxed num">
              {catalog.data.venues.map((v) => (
                <div key={v.exchangeId} className="flex justify-between gap-2">
                  <span>{venueName(v.exchangeId)}</span>
                  <span>
                    {v.rows} 个品种
                    {" · "}
                    {v.error
                      ? "刷新失败"
                      : v.updatedAt == null
                        ? "未就绪"
                        : `更新于 ${new Date(v.updatedAt).toLocaleString("zh-CN", { hour12: false })}`}
                  </span>
                </div>
              ))}
            </div>
            <div className="text-[13px] text-ds-text-3 mt-3 num break-all">
              数据目录：{catalog.data.dataDir}
            </div>
            <Button
              variant="secondary"
              size="sm"
              className="mt-3"
              onClick={() => void openDir(catalog.data!.dataDir)}
            >
              打开目录
            </Button>
          </>
        ) : null}
      </div>
      <div className={CARD}>
        <div className="text-base font-semibold text-ds-text-1 mb-1">日志</div>
        <div className="text-[13px] text-ds-text-2 mb-3 leading-relaxed">
          运行日志按大小轮转保存，遇到问题时把日志发给开发者。
        </div>
        {catalog.loading && !catalog.data ? (
          <PageState loading loadingText="加载中…" />
        ) : catalog.error && !catalog.data ? (
          <PageState error={`目录状态加载失败：${catalog.error}`} onRetry={() => catalog.refresh()} />
        ) : catalog.data ? (
          <>
            <div className="text-[13px] text-ds-text-3 num break-all">
              日志目录：{catalog.data.logDir}
            </div>
            <Button
              variant="secondary"
              size="sm"
              className="mt-3"
              onClick={() => void openDir(catalog.data!.logDir)}
            >
              打开目录
            </Button>
          </>
        ) : null}
      </div>
    </div>
  );
}
