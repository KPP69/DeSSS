import React from "react";
import { EmptyState, PageHeader } from "../components/ui";

export default function SystemPage() {
  return (
    <div>
      <PageHeader title="系统" />
      <EmptyState>空壳页面 — 接口契约已就绪，业务待接入（不接真实交易所）</EmptyState>
    </div>
  );
}
