//! Strategy commands: none yet. The Strategy module
//! (`strategy_engine`) has no Tauri commands; its surface is
//! internal, and order placement stays behind the execution
//! engine. This module exists so the domain split stays
//! complete (accounts / orders / market / risk / system /
//! strategy) when strategy commands arrive.
