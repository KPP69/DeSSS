import React from "react";
import { EmptyState } from "../components/ui";

export default function DataPage() {
  return (
    <div>
      <EmptyState>
        <div className="text-[13px] font-semibold text-ds-text-1">数据中心尚未接入</div>
        <div className="mt-1">后续阶段接入真实行情存储与回放后，历史数据与下载任务会显示在这里。</div>
      </EmptyState>
    </div>
  );
}
