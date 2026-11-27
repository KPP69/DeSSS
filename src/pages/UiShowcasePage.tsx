import React, { useLayoutEffect, useState } from "react";
import {
  Badge,
  Button,
  Checkbox,
  ConfirmDialog,
  DataTable,
  Input,
  Modal,
  MultiSelectDropdown,
  SelectDropdown,
  StatCard,
  Switch,
  Tooltip,
} from "../components/ui";
import PageState from "../components/PageState";
import { toast } from "../store/useToastStore";
import { useShellActions } from "../store/useShellActions";

/**
 * Component showcase (F2, item 14): every ui-kit component in every
 * state. Dev-only route `#/_ui` — not in the sidebar or any navigation;
 * kept in the production build for regression checks. Switch themes
 * in the sidebar to see both token sets.
 */
function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="mb-4 rounded-lg border border-ds-line bg-ds-panel p-4">
      <h2 className="text-[14px] font-semibold text-ds-text-1 mb-3">{title}</h2>
      {children}
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-start gap-3 py-2 border-t border-ds-line first:border-t-0 first:pt-0 last:pb-0">
      <div className="w-24 shrink-0 pt-1.5 text-xs text-ds-text-2">{label}</div>
      <div className="flex flex-wrap items-center gap-2 min-w-0">{children}</div>
    </div>
  );
}

const VARIANTS = ["primary", "secondary", "danger", "ghost"] as const;
const TONES = ["neutral", "up", "down", "warn", "info"] as const;
const OPTS = [
  { value: "hl", label: "Hyperliquid" },
  { value: "en", label: "Entropy" },
  { value: "li", label: "Lighter" },
  { value: "as", label: "AsterDEX" },
];

export default function UiShowcasePage() {
  const register = useShellActions((s) => s.register);
  const unregister = useShellActions((s) => s.unregister);
  useLayoutEffect(() => {
    register("_ui", { status: "开发预览 · 12 个组件 · 不进导航" });
    return () => unregister("_ui");
  }, [register, unregister]);

  const [sel, setSel] = useState("");
  const [multi, setMulti] = useState<string[]>(["hl", "en"]);
  const [checked, setChecked] = useState(true);
  const [modal, setModal] = useState(false);
  const [confirm, setConfirm] = useState(false);
  const [num, setNum] = useState("1234.56");
  const [sw, setSw] = useState(true);

  return (
    <div>
      <Section title="Button — 四变体 × 两尺寸">
        <Row label="md">
          {VARIANTS.map((v) => (
            <Button key={v} variant={v}>{v}</Button>
          ))}
        </Row>
        <Row label="sm">
          {VARIANTS.map((v) => (
            <Button key={v} variant={v} size="sm">{v}</Button>
          ))}
        </Row>
        <Row label="禁用">
          {VARIANTS.map((v) => (
            <Button key={v} variant={v} disabled>{v}</Button>
          ))}
        </Row>
        <Row label="加载中（宽度不跳）">
          <Button loading>保存</Button>
          <Button>保存</Button>
          <Button variant="secondary" loading>刷新</Button>
        </Row>
      </Section>

      <Section title="Input — text / number / password">
        <Row label="默认">
          <span className="w-56"><Input placeholder="输入标的，如 BTC" /></span>
          <span className="w-40"><Input type="number" value={num} onChange={(e) => setNum(e.target.value)} /></span>
          <span className="w-40"><Input type="password" placeholder="密码" /></span>
        </Row>
        <Row label="错误态">
          <span className="w-56"><Input defaultValue="0" error="价格必须大于 0" /></span>
        </Row>
        <Row label="禁用">
          <span className="w-56"><Input disabled placeholder="不可编辑" /></span>
        </Row>
      </Section>

      <Section title="SelectDropdown / MultiSelectDropdown">
        <Row label="单选">
          <span className="w-56"><SelectDropdown options={OPTS} value={sel} onChange={setSel} placeholder="请选择交易所" /></span>
          <span className="w-56"><SelectDropdown options={OPTS} value="hl" onChange={() => {}} /></span>
          <span className="w-56"><SelectDropdown options={OPTS} value="" onChange={() => {}} disabled placeholder="禁用" /></span>
        </Row>
        <Row label="多选">
          <span className="w-56"><MultiSelectDropdown options={OPTS} values={multi} onChange={setMulti} label="交易所" /></span>
          <span className="w-56"><MultiSelectDropdown options={OPTS} values={[]} onChange={() => {}} label="交易所" disabled /></span>
        </Row>
      </Section>

      <Section title="Checkbox">
        <Row label="状态">
          <Checkbox label="未选中" />
          <Checkbox label="已选中" checked={checked} onChange={(e) => setChecked(e.target.checked)} />
          <Checkbox label="禁用" disabled />
          <Checkbox label="禁用已选中" disabled checked />
        </Row>
      </Section>

      <Section title="Badge — 五 tone × 两尺寸">
        <Row label="sm / md">
          {TONES.map((t) => (
            <Badge key={t} tone={t}>{t}</Badge>
          ))}
          {TONES.map((t) => (
            <Badge key={t} tone={t} size="md">{t}</Badge>
          ))}
        </Row>
      </Section>

      <Section title="DataTable — 表头 / 行 hover 与焦点 / 空态">
        <Row label="有数据">
          <div className="flex-1 min-w-72">
            <DataTable
              headers={["标的", "价格", "涨跌"]}
              rows={[
                ["BTC", "90,123.45", "+1.24%"],
                ["ETH", "3,412.10", "-0.58%"],
                ["SOL", "212.88", "+3.02%"],
              ]}
            />
          </div>
        </Row>
        <Row label="空态">
          <div className="flex-1 min-w-72">
            <DataTable headers={["标的", "价格"]} rows={[]} emptyText="暂无持仓" />
          </div>
        </Row>
      </Section>

      <Section title="Modal / ConfirmDialog">
        <Row label="弹窗">
          <Button onClick={() => setModal(true)}>打开 Modal（md）</Button>
          <Button variant="secondary" onClick={() => setConfirm(true)}>打开 ConfirmDialog</Button>
          <span className="text-xs text-ds-text-3">Esc 关闭、点遮罩关闭、关闭后焦点回到触发按钮</span>
        </Row>
      </Section>

      <Section title="Toast — 成功 / 失败 / 排队">
        <Row label="触发">
          <Button variant="secondary" onClick={() => toast.success("保存成功")}>成功一条</Button>
          <Button variant="secondary" onClick={() => toast.error("后端返回：余额不足")}>失败一条</Button>
          <Button variant="secondary" onClick={() => { for (let i = 1; i <= 4; i++) toast.error(`排队第 ${i} 条`); }}>连发 4 条（第 4 条排队）</Button>
          <span className="text-xs text-ds-text-3">右上角，3 秒消失，点按关闭</span>
        </Row>
      </Section>

      <Section title="PageState — loading / error / empty">
        <Row label="三态">
          <div className="flex-1 min-w-48"><PageState loading loadingText="加载中…" /></div>
          <div className="flex-1 min-w-48"><PageState error="行情加载失败" onRetry={() => toast.success("已重试")} /></div>
          <div className="flex-1 min-w-48"><PageState empty><span className="text-ds-text-3">暂无策略</span></PageState></div>
        </Row>
      </Section>

      <Section title="StatCard">
        <Row label="卡片">
          <div className="w-48"><StatCard title="总权益" value="$12,483.20" sub="全部账户" /></div>
          <div className="w-48"><StatCard title="今日盈亏" value="+$182.40" tone="profit" change={{ text: "+1.48% 今日", tone: "up" }} /></div>
          <div className="w-48"><StatCard title="今日盈亏" value="-$96.10" tone="loss" change={{ text: "-0.77% 今日", tone: "down" }} /></div>
        </Row>
      </Section>

      <Section title="Switch — 开关（F3 新增）">
        <Row label="sm / lg">
          <Switch checked={sw} onChange={setSw} label="演示开关" />
          <Switch checked={false} onChange={() => {}} label="演示开关（关）" />
          <Switch checked disabled onChange={() => {}} label="演示开关（禁用）" />
          <Switch size="lg" checked={sw} onChange={setSw} label="演示大开关" />
        </Row>
      </Section>

      <Section title="Tooltip — 400ms 悬停 / 聚焦即现">
        <Row label="提示">
          <Tooltip content="仅解释图标或截断文本，不承载关键信息">
            <Button variant="ghost" size="sm">悬停 / 聚焦我</Button>
          </Tooltip>
          <Tooltip content="Hyperliquid 主网永续，BTC 标的">
            <span className="max-w-28 truncate text-[13px] text-ds-text-1">Hyperliquid-BTC-永续-很长很长</span>
          </Tooltip>
        </Row>
      </Section>

      <Modal open={modal} title="演示弹窗" onClose={() => setModal(false)} footer={<Button onClick={() => setModal(false)}>知道了</Button>}>
        <p className="text-[13px] text-ds-text-2">打开时焦点进入弹窗、滚动锁定；Esc 或点遮罩关闭后，焦点回到「打开 Modal」按钮。</p>
      </Modal>
      <ConfirmDialog
        open={confirm}
        title="删除策略"
        message="删除后不可恢复，确认删除该策略吗？"
        confirmText="确认删除"
        onConfirm={async () => {
          await new Promise((r) => setTimeout(r, 600));
          setConfirm(false);
          toast.success("已删除（演示）");
        }}
        onCancel={() => setConfirm(false)}
      />
    </div>
  );
}
