---
title: Plugins
icon: lucide/plug
---

# Plugins

On top of general task specifications like scheduler related ones, the actual behavior of
tasks is implemented in so-called task plugins. Two of them are currently available, namely [`shell`](shell.md) and [`icon`](icon.md). The former can be considered as an **all rounder** task where users bring their own set of files (typically scripts) and the latter, as the name indicates, is specifically designed as an **interface to the ICON model**.

Specifications corresponding to a particular plugin are listed at the same level as [the ones applying to any task](../configuration/tasks.md) like `plugin` itself.
