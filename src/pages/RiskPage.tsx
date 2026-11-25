import React from "react";
import { EmptyState } from "../components/ui";

export default function RiskPage() {
  return (
    <div>
      <EmptyState>
        <div className="text-[13px] font-semibold text-ds-text-1">风控引擎尚未接入</div>
        <div className="mt-1">后续阶段接入真实风控引擎后，限额、敞口与风控检查会显示在这里。</div>
      </EmptyState>
    </div>
  );
}
