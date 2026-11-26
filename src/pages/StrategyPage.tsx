import React from "react";
import PageState from "../components/PageState";

/** Strategy shell (Phase 2, I11): still empty, but on the same
 * skeleton as every other page — the real engine lands in Phase 3. */
export default function StrategyPage() {
  return (
    <div>
      <PageState empty>
        <div className="text-[13px] font-semibold text-ds-text-1">策略引擎尚未接入</div>
        <div className="mt-1">后续阶段接入真实策略引擎后，策略列表与执行状态会显示在这里。</div>
      </PageState>
    </div>
  );
}
