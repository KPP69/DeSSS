import React from "react";
import { EmptyState } from "../components/ui";

export default function StrategyPage() {
  return (
    <div>
      <EmptyState>
        <div className="text-[13px] font-semibold text-ds-text-1">策略引擎尚未接入</div>
        <div className="mt-1">后续阶段接入真实策略引擎后，策略列表与执行状态会显示在这里。</div>
      </EmptyState>
    </div>
  );
}
