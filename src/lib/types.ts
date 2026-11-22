export type Tick={adapterId:string;symbol:string;bid:number;ask:number;last:number;volume24h:number;ts:number};
export type Balance={adapterId:string;accountId:string;asset:string;free:number;locked:number;total:number;equityUsd:number;ts:number};
export type Position={adapterId:string;accountId:string;symbol:string;side:string;qty:number;entryPrice:number;markPrice:number;unrealizedPnl:number};
export type AppInfo={name:string;version:string;dataDir:string;dbPath:string;portable:boolean};
export type LegBinding={legId:string;adapterId:string;accountId:string;symbol:string;ratio:number;side:string;enabled:boolean};
