#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Single depth-parameterised model: d graph-propagation layers + persistence residual.

d=2 is bit-compatible with the frozen checkpoints (w1, w2, wo).
"""
import torch
import torch.nn as nn


class GraphDepth(nn.Module):
    def __init__(self, L, H, C=64, depth=2):
        super().__init__()
        assert depth >= 1
        self.L, self.H, self.C, self.depth = L, H, C, depth
        ws = [nn.Parameter(torch.randn(L, C) * (2.0 / L) ** 0.5)]
        for _ in range(depth - 1):
            ws.append(nn.Parameter(torch.randn(C, C) * (2.0 / C) ** 0.5))
        self.ws = nn.ParameterList(ws)
        self.wo = nn.Parameter(torch.randn(C, H) * (2.0 / C) ** 0.5)

    def forward(self, A, x):                      # x [B,N,L]
        h = torch.relu(torch.matmul(A, x @ self.ws[0]))
        for i in range(1, self.depth):
            h = torch.relu(torch.matmul(A, h @ self.ws[i]))
        return x[:, :, -1:].expand(-1, -1, self.H) + h @ self.wo

    def n_params(self):
        return int(sum(p.numel() for p in self.parameters()))


def load_legacy(state):
    """Map a frozen 2-layer checkpoint (w1, w2, wo) into GraphDepth(depth=2)."""
    m = GraphDepth(L=state["w1"].shape[0], H=state["wo"].shape[1], C=state["w1"].shape[1], depth=2)
    with torch.no_grad():
        m.ws[0].copy_(state["w1"]); m.ws[1].copy_(state["w2"]); m.wo.copy_(state["wo"])
    return m


def state_from_legacy(state):
    return {"ws.0": state["w1"], "ws.1": state["w2"], "wo": state["wo"]}
