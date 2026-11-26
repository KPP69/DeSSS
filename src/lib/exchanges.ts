/**
 * Exchange credential form definitions.
 *
 * Each exchange owns an independent field set; switching the exchange in the
 * account modal re-renders exactly these fields. The Rust backend validates
 * the same rules again — the frontend config is for UX, not for security.
 */

export type FieldDef = {
  key: string;
  label: string;
  type: "text" | "password" | "number";
  required?: boolean;
  hint?: string;
  min?: number;
  max?: number;
  /** Values that fall inside [min, max] but are still not allowed. */
  forbidden?: number[];
  forbiddenMessage?: string;
};

export type CredentialMode = {
  id: string;
  label: string;
  fields: FieldDef[];
};

export type ExchangeDef = {
  id: string;
  name: string;
  note?: string;
  fields?: FieldDef[];
  /** When present, the form shows a mode switch (e.g. AsterDEX). */
  modes?: CredentialMode[];
};

const walletFields: FieldDef[] = [
  { key: "wallet_address", label: "主钱包地址", type: "text", required: true },
  { key: "agent_private_key", label: "Agent/API 钱包私钥", type: "password", required: true },
];

export const EXCHANGES: ExchangeDef[] = [
  {
    id: "hyperliquid",
    name: "Hyperliquid",
    fields: walletFields,
  },
  {
    id: "entropy",
    name: "Entropy",
    note: "复用 Hyperliquid HIP-3 接入模式",
    fields: walletFields,
  },
  {
    id: "lighter",
    name: "Lighter",
    fields: [
      { key: "account_index", label: "Account Index", type: "number", required: true, min: 0 },
      {
        key: "api_key_index",
        label: "API Key Index",
        type: "number",
        required: true,
        min: 4,
        max: 254,
        hint: "允许范围 4–254",
      },
      { key: "api_key_private_key", label: "API Key 私钥", type: "password", required: true },
    ],
  },
  {
    id: "lighter_robinhood",
    name: "Lighter RH",
    fields: [
      { key: "account_index", label: "Account Index", type: "number", required: true, min: 0 },
      {
        key: "api_key_index",
        label: "API Key Index",
        type: "number",
        required: true,
        min: 4,
        max: 254,
        forbidden: [157],
        forbiddenMessage: "157 是保留位，不能使用",
        hint: "允许范围 4–156 或 158–254，请避开保留位 157",
      },
      { key: "api_key_private_key", label: "API Key 私钥", type: "password", required: true },
    ],
  },
  {
    id: "asterdex",
    name: "AsterDEX",
    note: "V3 仅支持 Agent 钱包凭证",
    modes: [
      {
        id: "agent",
        label: "Agent 钱包",
        fields: [
          { key: "master_wallet_address", label: "主钱包地址", type: "text", required: true, hint: "拥有该账户、授权了 Agent 的主钱包地址（V3 签名 user 参数）" },
          { key: "agent_wallet_address", label: "Agent 钱包地址", type: "text", required: true },
          { key: "agent_private_key", label: "Agent 钱包私钥", type: "password", required: true },
        ],
      },
    ],
  },
  {
    id: "arcus",
    name: "Arcus",
    fields: [
      { key: "wallet_address", label: "钱包地址", type: "text", required: true },
      { key: "signing_private_key", label: "交易签名私钥/凭证", type: "password", required: true },
    ],
  },
];

export function getExchange(id: string): ExchangeDef {
  return EXCHANGES.find((e) => e.id === id) ?? EXCHANGES[0];
}

export function activeFields(def: ExchangeDef, modeId: string): FieldDef[] {
  if (def.modes) {
    return def.modes.find((m) => m.id === modeId)?.fields ?? def.modes[0].fields;
  }
  return def.fields ?? [];
}

/** Validate one field value; returns an error message or null. */
export function validateField(field: FieldDef, raw: string): string | null {
  const value = raw.trim();
  if (field.required && value === "") return `请填写${field.label}`;
  if (value === "") return null;
  if (field.type === "number") {
    const n = Number(value);
    if (!Number.isFinite(n) || !Number.isInteger(n)) return `${field.label} 必须是整数`;
    if (field.min !== undefined && n < field.min) return `${field.label} 不能小于 ${field.min}`;
    if (field.max !== undefined && n > field.max) return `${field.label} 不能大于 ${field.max}`;
    if (field.forbidden?.includes(n)) return field.forbiddenMessage ?? `${field.label} 的值 ${n} 不可用`;
  }
  return null;
}
