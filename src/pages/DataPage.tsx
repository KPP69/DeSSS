import React from "react"; 
const Empty=()=> (<div className="border border-dashed border-slate-700 rounded p-8 text-slate-500 text-sm">空壳页面 — 接口契约已就绪，业务待接入（不接真实交易所）</div>);
export default function DataPage(){  return (<div><h1 className="text-xl font-semibold mb-4">数据</h1><Empty/></div>); }
