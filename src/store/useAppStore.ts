import { create } from "zustand"; export type PageKey="account"|"strategy"|"trading"|"data"|"risk"|"system"|"_ui";
type S={page:PageKey; setPage:(p:PageKey)=>void}; export const useAppStore=create<S>((set)=>({page:"trading",setPage:(p)=>set({page:p})}));
