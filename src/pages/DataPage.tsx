import React from "react";
import { EmptyState, PageHeader } from "../components/ui";

export default function DataPage() {
  return (
    <div>
      <PageHeader title="数据" />
      <EmptyState>空壳页面 — 接口契约已就绪，业务待接入（不接真实交易所）</EmptyState>
    </div>
  );
}
