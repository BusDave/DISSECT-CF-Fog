"""Pure-PyTorch GNN model — ONNX-compatible. No PyTorch Geometric.

Architecture: GraphSAGE-style two-stream (workflow + cluster) encoder,
plus a cross-attention scorer that turns the current-task embedding
into a score per cluster node.

Message passing per layer:
    h_v^(l+1) = ReLU( W_self · h_v^(l)  +  W_nbr · (A_norm @ H^(l))_v )
where A_norm = D^{-1/2} (A + I) D^{-1/2} (precomputed in the pipeline).
This is implemented as two `nn.Linear` calls plus a batched dense matmul,
all of which export cleanly to ONNX opset 17.
"""
from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F


class GraphSageBlock(nn.Module):
    """One message-passing layer:
       out = ReLU( lin_self(h) + lin_nbr(adj @ h) )
    Supports batched input: h shape [B, N, F_in], adj [B, N, N]."""

    def __init__(self, in_dim: int, out_dim: int):
        super().__init__()
        self.lin_self = nn.Linear(in_dim, out_dim)
        self.lin_nbr  = nn.Linear(in_dim, out_dim)

    def forward(self, h: torch.Tensor, adj: torch.Tensor) -> torch.Tensor:
        agg = torch.bmm(adj, h)                      # [B, N, F_in]
        return F.relu(self.lin_self(h) + self.lin_nbr(agg))


class GnnScheduler(nn.Module):
    """Two-stream GNN: workflow encoder + cluster encoder + cross scorer.

    Inputs (training / eval, batched):
        wf_node_features [B, N_t, F_t]  — F_t = 6
        wf_adj_norm      [B, N_t, N_t]
        cl_node_features [B, N_n, F_n]  — F_n = 8
        cl_adj_norm      [B, N_n, N_n]
        current_task_idx [B]            — long, index into wf nodes

    Output:
        scores [B, N_n]  — one score per cluster node (softmax outside)

    For ONNX export (single-sample), the same forward works with B=1.
    """

    def __init__(self,
                 wf_in_dim: int = 6,
                 cl_in_dim: int = 8,
                 hidden_dim: int = 64,
                 num_layers: int = 3):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers

        self.wf_layers = nn.ModuleList()
        self.cl_layers = nn.ModuleList()
        for i in range(num_layers):
            in_wf = wf_in_dim if i == 0 else hidden_dim
            in_cl = cl_in_dim if i == 0 else hidden_dim
            self.wf_layers.append(GraphSageBlock(in_wf, hidden_dim))
            self.cl_layers.append(GraphSageBlock(in_cl, hidden_dim))

        # Cross-attention-style scorer: query = current-task embedding,
        # key = cluster embeddings → score per cluster node.
        self.query_proj = nn.Linear(hidden_dim, hidden_dim)
        self.key_proj   = nn.Linear(hidden_dim, hidden_dim)
        self.score_head = nn.Linear(hidden_dim, 1)
        self.dropout    = nn.Dropout(0.1)

    def encode_workflow(self, h: torch.Tensor, adj: torch.Tensor) -> torch.Tensor:
        for layer in self.wf_layers:
            h = layer(h, adj)
            h = self.dropout(h)
        return h                                       # [B, N_t, H]

    def encode_cluster(self, h: torch.Tensor, adj: torch.Tensor) -> torch.Tensor:
        for layer in self.cl_layers:
            h = layer(h, adj)
            h = self.dropout(h)
        return h                                       # [B, N_n, H]

    def forward(self,
                wf_node_features: torch.Tensor,
                wf_adj_norm:      torch.Tensor,
                cl_node_features: torch.Tensor,
                cl_adj_norm:      torch.Tensor,
                current_task_idx: torch.Tensor) -> torch.Tensor:
        wf_emb = self.encode_workflow(wf_node_features, wf_adj_norm)   # [B, N_t, H]
        cl_emb = self.encode_cluster(cl_node_features, cl_adj_norm)    # [B, N_n, H]

        # Gather the current-task embedding from each batch element.
        # current_task_idx shape [B] -> expanded to [B, 1, H] for gather along dim=1.
        idx = current_task_idx.unsqueeze(-1).unsqueeze(-1).expand(-1, 1, self.hidden_dim)
        task_emb = torch.gather(wf_emb, 1, idx).squeeze(1)             # [B, H]

        q = self.query_proj(task_emb).unsqueeze(1)                     # [B, 1, H]
        k = self.key_proj(cl_emb)                                       # [B, N_n, H]

        # Scaled dot-product score
        scale = self.hidden_dim ** 0.5
        dot   = (q * k).sum(dim=-1) / scale                            # [B, N_n]

        # Additive head on the cluster embedding alone (gives the model a
        # node-bias term independent of the current task — useful for "this
        # node is overloaded regardless").
        head = self.score_head(cl_emb).squeeze(-1)                     # [B, N_n]

        return dot + head                                              # [B, N_n]


# =============================================================================
#  ONNX export wrapper — converts dynamic batch to single-sample inference
# =============================================================================

class GnnSchedulerInference(nn.Module):
    """Thin wrapper around GnnScheduler for ONNX export.

    Java passes ONE sample at a time, so we accept un-batched tensors and
    add the batch dim internally. The exported model takes:
        wf_node_features [N_t, 6]
        wf_adj_norm      [N_t, N_t]
        cl_node_features [N_n, 8]
        cl_adj_norm      [N_n, N_n]
        current_task_idx [1]
    Returns: scores [N_n]
    """

    def __init__(self, base: GnnScheduler):
        super().__init__()
        self.base = base

    def forward(self,
                wf_node_features: torch.Tensor,
                wf_adj_norm:      torch.Tensor,
                cl_node_features: torch.Tensor,
                cl_adj_norm:      torch.Tensor,
                current_task_idx: torch.Tensor) -> torch.Tensor:
        # Add batch dim
        wfn = wf_node_features.unsqueeze(0)
        wfa = wf_adj_norm.unsqueeze(0)
        cln = cl_node_features.unsqueeze(0)
        cla = cl_adj_norm.unsqueeze(0)
        scores = self.base(wfn, wfa, cln, cla, current_task_idx)        # [1, N_n]
        return scores.squeeze(0)                                        # [N_n]
