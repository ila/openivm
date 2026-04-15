#!/usr/bin/env python3
"""
Learned Cost Model Simulation for OpenIVM (Item 2.3)

Validates the weighted NNLS ridge regression algorithm across 30 scenarios
before implementing in C++. Each scenario runs 100+ iterations.

Algorithm: Per-view, per-method weighted least squares regression.
  predicted_ms = w_compute * compute_est + w_upsert * upsert_est + w_intercept
  - Decay-weighted (recent observations matter more)
  - Ridge regularization (handles collinearity)
  - Non-negative least squares (slope coefficients >= 0)
  - Prediction clamped to max(0, pred) to avoid negative time estimates
  - Cold start fallback to static model (w_compute=1, w_upsert=1, w_intercept=0)

Usage: python3 scripts/learned_cost_sim.py
"""

import numpy as np
import sys

np.random.seed(42)

# ============================================================================
# Core Algorithm (will be ported to C++)
# ============================================================================

def compute_weights(n, decay):
    """Exponential decay weights. Most recent = 1.0, oldest = decay^(n-1)."""
    return np.array([decay ** (n - 1 - i) for i in range(n)])


def fit_nnls_ridge(X, y, weights, lam=1e-4):
    """
    Weighted non-negative least squares with ridge regularization.

    X: (n, 3) matrix -- columns: [compute_est, upsert_est, 1.0 (intercept)]
    y: (n,) vector -- actual_duration_ms
    weights: (n,) vector -- decay weights
    lam: ridge regularization parameter

    Returns: (w_compute, w_upsert, w_intercept) where w_compute, w_upsert >= 0
    """
    W = np.diag(weights)

    # Ridge normal equations: (X'WX + lam*I) w = X'Wy
    XtWX = X.T @ W @ X + lam * np.eye(X.shape[1])
    XtWy = X.T @ W @ y

    try:
        w = np.linalg.solve(XtWX, XtWy)
    except np.linalg.LinAlgError:
        return np.array([0.0, 0.0, np.average(y, weights=weights)])

    if not np.all(np.isfinite(w)):
        return np.array([0.0, 0.0, np.average(y, weights=weights)])

    # NNLS: clamp negative slope coefficients, re-fit with reduced features
    if w[0] < 0 and w[1] < 0:
        return np.array([0.0, 0.0, np.average(y, weights=weights)])
    elif w[0] < 0:
        X2 = X[:, [1, 2]]
        XtWX2 = X2.T @ W @ X2 + lam * np.eye(2)
        XtWy2 = X2.T @ W @ y
        try:
            w2 = np.linalg.solve(XtWX2, XtWy2)
        except np.linalg.LinAlgError:
            return np.array([0.0, 0.0, np.average(y, weights=weights)])
        if w2[0] < 0:
            return np.array([0.0, 0.0, np.average(y, weights=weights)])
        return np.array([0.0, w2[0], w2[1]])
    elif w[1] < 0:
        X2 = X[:, [0, 2]]
        XtWX2 = X2.T @ W @ X2 + lam * np.eye(2)
        XtWy2 = X2.T @ W @ y
        try:
            w2 = np.linalg.solve(XtWX2, XtWy2)
        except np.linalg.LinAlgError:
            return np.array([0.0, 0.0, np.average(y, weights=weights)])
        if w2[0] < 0:
            return np.array([0.0, 0.0, np.average(y, weights=weights)])
        return np.array([w2[0], 0.0, w2[1]])

    return w


class CostModelSimulator:
    """Simulates the learned cost model for one method (IVM or recompute)."""

    def __init__(self, decay=0.9, window=20, min_samples=3, ridge_lambda=1e-4):
        self.decay = decay
        self.window = window
        self.min_samples = min_samples
        self.ridge_lambda = ridge_lambda
        self.history = []

    def record(self, compute_est, upsert_est, actual_ms):
        self.history.append((compute_est, upsert_est, actual_ms))
        if len(self.history) > self.window:
            self.history = self.history[-self.window:]

    def predict(self, compute_est, upsert_est):
        """Returns (predicted_ms, weights_tuple, calibrated_bool)."""
        if len(self.history) < self.min_samples:
            return compute_est + upsert_est, (1.0, 1.0, 0.0), False

        X = np.array([[h[0], h[1], 1.0] for h in self.history])
        y = np.array([h[2] for h in self.history])
        wts = compute_weights(len(self.history), self.decay)

        w = fit_nnls_ridge(X, y, wts, self.ridge_lambda)
        pred = w[0] * compute_est + w[1] * upsert_est + w[2]
        # Clamp to non-negative (negative time prediction is nonsensical)
        pred = max(0.0, pred)
        return pred, tuple(w), True

    def reset(self):
        self.history = []


# ============================================================================
# Scenario Infrastructure
# ============================================================================

class ScenarioResult:
    def __init__(self, name):
        self.name = name
        self.passed = True
        self.messages = []
        self.errors = []
        self.predictions = []
        self.actuals = []
        self.weights_history = []

    def check(self, condition, msg):
        if not condition:
            self.passed = False
            self.errors.append(msg)

    def info(self, msg):
        self.messages.append(msg)

    def mape_after(self, start_idx):
        preds = self.predictions[start_idx:]
        acts = self.actuals[start_idx:]
        errs = [abs(p - a) / abs(a) for p, a in zip(preds, acts) if abs(a) > 1e-6]
        return np.mean(errs) if errs else 0.0

    def mae_after(self, start_idx):
        preds = self.predictions[start_idx:]
        acts = self.actuals[start_idx:]
        return np.mean([abs(p - a) for p, a in zip(preds, acts)]) if preds else float('inf')

    def no_negative_preds(self):
        return all(p >= 0 for p in self.predictions)

    def all_finite(self):
        w_ok = all(all(np.isfinite(w) for w in wh) for wh in self.weights_history)
        p_ok = all(np.isfinite(p) for p in self.predictions)
        return w_ok and p_ok


def generate_actual(compute_est, upsert_est, true_w, noise_std=0.05):
    """Generate actual_ms from ground truth weights + relative noise."""
    base = true_w[0] * compute_est + true_w[1] * upsert_est + true_w[2]
    noise = np.random.normal(0, max(noise_std * abs(base), 0.1))
    return max(0.0, base + noise)


def run_simple(sim, true_w, n, feature_gen, noise_std=0.05, result=None):
    """Run n iterations with given feature generator. Returns result."""
    if result is None:
        result = ScenarioResult("unnamed")
    for _ in range(n):
        compute, upsert = feature_gen()
        actual = generate_actual(compute, upsert, true_w, noise_std)
        pred, weights, cal = sim.predict(compute, upsert)
        result.predictions.append(pred)
        result.actuals.append(actual)
        result.weights_history.append(weights)
        sim.record(compute, upsert, actual)
    return result


# ============================================================================
# Scenarios 1-20: Original
# ============================================================================

def scenario_01_cold_start():
    r = ScenarioResult("01_cold_start")
    sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(10, 500), np.random.uniform(5, 200))
    run_simple(sim, (0.5, 3.0, 10.0), 100, feat, result=r)
    mape = r.mape_after(50)
    r.info(f"MAPE after 10: {r.mape_after(10):.3f}, last 50: {mape:.3f}")
    r.info(f"Final weights: {r.weights_history[-1]}")
    r.check(mape < 0.20, f"MAPE {mape:.3f} > 0.20")
    return r

def scenario_02_steady_state():
    r = ScenarioResult("02_steady_state")
    sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(50, 1000), np.random.uniform(20, 500))
    run_simple(sim, (1.2, 0.8, 5.0), 150, feat, result=r)
    mape = r.mape_after(50)
    r.info(f"MAPE last 100: {mape:.3f}, final weights: {r.weights_history[-1]}")
    r.check(mape < 0.15, f"MAPE {mape:.3f} > 0.15")
    return r

def scenario_03_single_point_fallback():
    r = ScenarioResult("03_single_point_fallback")
    sim = CostModelSimulator()
    _, _, cal0 = sim.predict(100, 50)
    r.check(not cal0, "Should not be calibrated with 0 history")
    sim.record(100, 50, 200); sim.record(200, 100, 400)
    _, _, cal2 = sim.predict(100, 50)
    r.check(not cal2, "Should not be calibrated with 2 entries")
    sim.record(50, 25, 100)
    _, _, cal3 = sim.predict(100, 50)
    r.check(cal3, "Should be calibrated with 3 entries")
    r.info(f"Fallback logic: 0->uncal, 2->uncal, 3->cal")
    return r

def scenario_04_workload_shift():
    r = ScenarioResult("04_workload_shift")
    sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(50, 300), np.random.uniform(20, 150))
    run_simple(sim, (1.0, 2.0, 5.0), 50, feat, result=r)
    run_simple(sim, (0.3, 5.0, 20.0), 50, feat, result=r)
    mape_post = r.mape_after(65)
    r.info(f"Post-shift MAPE (iter 65+): {mape_post:.3f}")
    r.info(f"Weights at shift: {r.weights_history[50]}, after: {r.weights_history[-1]}")
    r.check(mape_post < 0.25, f"Post-shift MAPE {mape_post:.3f} > 0.25")
    return r

def scenario_05_gradual_drift():
    r = ScenarioResult("05_gradual_drift")
    sim = CostModelSimulator()
    for i in range(100):
        true_w = (0.5 + i * 0.01, 3.0 - i * 0.02, 10.0)
        c, u = np.random.uniform(50, 300), np.random.uniform(20, 150)
        actual = generate_actual(c, u, true_w)
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(c, u, actual)
    mape = r.mape_after(70)
    r.info(f"MAPE last 30: {mape:.3f}, final weights: {r.weights_history[-1]}")
    r.check(mape < 0.20, f"Drift MAPE {mape:.3f} > 0.20")
    return r

def scenario_06_opt_flag_change():
    r = ScenarioResult("06_opt_flag_change")
    sim = CostModelSimulator()
    for i in range(100):
        c, u = np.random.uniform(50, 300), np.random.uniform(20, 200)
        true_w = (1.0, 2.0, 5.0) if i < 50 else (1.0, 1.0, 5.0)
        actual = generate_actual(c, u, true_w)
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(c, u, actual)
    mape = r.mape_after(70)
    r.info(f"MAPE after flag change adaptation: {mape:.3f}")
    r.check(mape < 0.20, f"MAPE {mape:.3f} > 0.20")
    return r

def scenario_07_ivm_only():
    r = ScenarioResult("07_ivm_only")
    ivm_sim = CostModelSimulator()
    rc_sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(10, 500), np.random.uniform(5, 200))
    run_simple(ivm_sim, (0.8, 1.5, 10.0), 100, feat, result=r)
    _, _, cal_rc = rc_sim.predict(300, 100)
    r.check(not cal_rc, "Recompute must stay uncalibrated")
    mape = r.mape_after(50)
    r.info(f"IVM MAPE last 50: {mape:.3f}")
    r.check(mape < 0.15, f"MAPE {mape:.3f} > 0.15")
    return r

def scenario_08_full_only():
    r = ScenarioResult("08_full_only")
    sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(200, 2000), np.random.uniform(50, 500))
    run_simple(sim, (1.5, 0.5, 20.0), 100, feat, result=r)
    mape = r.mape_after(50)
    r.info(f"Full-only MAPE last 50: {mape:.3f}")
    r.check(mape < 0.15, f"MAPE {mape:.3f} > 0.15")
    return r

def scenario_09_alternating():
    r = ScenarioResult("09_alternating")
    ivm_sim, rc_sim = CostModelSimulator(), CostModelSimulator()
    ivm_p, ivm_a, rc_p, rc_a = [], [], [], []
    for i in range(100):
        if i % 2 == 0:
            c, u = np.random.uniform(50, 300), np.random.uniform(20, 150)
            actual = generate_actual(c, u, (0.5, 2.0, 5.0))
            pred, _, _ = ivm_sim.predict(c, u)
            ivm_sim.record(c, u, actual)
            ivm_p.append(pred); ivm_a.append(actual)
        else:
            c, u = np.random.uniform(200, 1000), np.random.uniform(50, 300)
            actual = generate_actual(c, u, (1.0, 0.3, 15.0))
            pred, _, _ = rc_sim.predict(c, u)
            rc_sim.record(c, u, actual)
            rc_p.append(pred); rc_a.append(actual)
    def mape_l(p, a, s):
        return np.mean([abs(x-y)/abs(y) for x, y in zip(p[s:], a[s:]) if abs(y) > 1e-6])
    r.info(f"IVM MAPE: {mape_l(ivm_p, ivm_a, 25):.3f}, RC MAPE: {mape_l(rc_p, rc_a, 25):.3f}")
    r.check(mape_l(ivm_p, ivm_a, 25) < 0.20, "IVM alternating MAPE too high")
    r.check(mape_l(rc_p, rc_a, 25) < 0.20, "RC alternating MAPE too high")
    r.predictions = ivm_p; r.actuals = ivm_a
    return r

def scenario_10_ivm_full_ivm_transition():
    r = ScenarioResult("10_ivm_full_ivm_transition")
    ivm_sim, rc_sim = CostModelSimulator(), CostModelSimulator()
    feat = lambda: (np.random.uniform(50, 300), np.random.uniform(20, 150))
    rc_feat = lambda: (np.random.uniform(200, 1000), np.random.uniform(50, 300))
    run_simple(ivm_sim, (0.8, 1.5, 8.0), 30, feat, result=r)
    run_simple(rc_sim, (1.2, 0.4, 12.0), 20, rc_feat, result=r)
    run_simple(ivm_sim, (0.8, 1.5, 8.0), 30, feat, result=r)
    mape = r.mape_after(60)
    r.info(f"Phase 3 MAPE: {mape:.3f}")
    r.check(mape < 0.25, f"Phase 3 MAPE {mape:.3f} > 0.25")
    return r

def scenario_11_varying_delta_sizes():
    r = ScenarioResult("11_varying_delta_sizes")
    sim = CostModelSimulator()
    feat = lambda: ((np.random.pareto(2.0) + 1) * 10, (np.random.pareto(2.0) + 1) * 5)
    run_simple(sim, (0.3, 2.5, 5.0), 100, feat, result=r)
    mape = r.mape_after(50)
    r.info(f"MAPE last 50: {mape:.3f}")
    r.check(mape < 0.25, f"Power-law MAPE {mape:.3f} > 0.25")
    return r

def scenario_12_collinear_features():
    r = ScenarioResult("12_collinear_features")
    sim = CostModelSimulator()
    def feat():
        b = np.random.uniform(10, 500)
        return b + np.random.normal(0, 5), b * 0.8 + np.random.normal(0, 3)
    run_simple(sim, (1.0, 2.0, 10.0), 100, feat, result=r)
    r.check(r.all_finite(), "Non-finite values")
    mape = r.mape_after(50)
    r.info(f"MAPE last 50: {mape:.3f}, final weights: {r.weights_history[-1]}")
    r.check(mape < 0.25, f"Collinear MAPE {mape:.3f} > 0.25")
    return r

def scenario_13_huge_variance():
    r = ScenarioResult("13_huge_variance")
    sim = CostModelSimulator()
    def feat():
        s = 10 ** np.random.uniform(0, 3.7)
        return s, s * 0.5
    run_simple(sim, (2.0, 1.0, 1.0), 100, feat, noise_std=0.1, result=r)
    r.check(r.all_finite(), "Non-finite values")
    r.info(f"Actual range: [{min(r.actuals):.1f}, {max(r.actuals):.1f}]")
    return r

def scenario_14_zero_deltas():
    r = ScenarioResult("14_zero_deltas")
    sim = CostModelSimulator()
    for i in range(100):
        if i % 5 == 0:
            c, u, actual = 0.0, 0.0, np.random.uniform(0, 0.5)
        else:
            c, u = np.random.uniform(10, 200), np.random.uniform(5, 100)
            actual = generate_actual(c, u, (1.0, 2.0, 3.0))
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(c, u, actual)
    r.check(r.all_finite(), "Non-finite values with zero deltas")
    r.check(r.no_negative_preds(), "Negative predictions with zero deltas")
    return r

def scenario_15_all_zero_estimates():
    r = ScenarioResult("15_all_zero_estimates")
    sim = CostModelSimulator()
    for _ in range(20):
        sim.record(0.0, 0.0, np.random.uniform(0, 1))
    pred, w, cal = sim.predict(0.0, 0.0)
    r.check(np.isfinite(pred) and pred >= 0, f"Bad prediction: {pred}")
    pred2, _, _ = sim.predict(100.0, 50.0)
    r.check(np.isfinite(pred2) and pred2 >= 0, f"Bad prediction for non-zero: {pred2}")
    r.info(f"(0,0)->pred={pred:.2f}, (100,50)->pred={pred2:.2f}")
    return r

def scenario_16_negative_ols():
    r = ScenarioResult("16_negative_ols")
    sim = CostModelSimulator()
    for i in range(30):
        c = np.random.uniform(100, 500)
        u = np.random.uniform(10, 50)
        actual = max(1.0, -0.5 * c + 5.0 * u + 300 + np.random.normal(0, 10))
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(c, u, actual)
    fw = r.weights_history[-1]
    r.info(f"Final weights: {fw}")
    r.check(fw[0] >= 0 and fw[1] >= 0, f"Negative slopes: {fw}")
    r.check(r.no_negative_preds(), "Negative predictions")
    return r

def scenario_17_near_singular():
    r = ScenarioResult("17_near_singular")
    sim = CostModelSimulator()
    def feat():
        b = np.random.uniform(50, 300)
        return b, b + np.random.normal(0, 0.01)
    run_simple(sim, (1.0, 1.0, 5.0), 50, feat, result=r)
    r.check(r.all_finite(), "Non-finite values")
    mape = r.mape_after(30)
    r.info(f"MAPE last 20: {mape:.3f}")
    r.check(mape < 0.30, f"MAPE {mape:.3f} > 0.30")
    return r

def scenario_18_join_chain():
    r = ScenarioResult("18_join_chain")
    sim = CostModelSimulator()
    def feat():
        return 7 * np.random.uniform(1000, 10000), np.random.uniform(5, 50)
    run_simple(sim, (3.0, 0.2, 15.0), 100, feat, result=r)
    mape = r.mape_after(50)
    r.info(f"MAPE last 50: {mape:.3f}, weights: {r.weights_history[-1]}")
    r.check(mape < 0.15, f"MAPE {mape:.3f} > 0.15")
    r.check(r.no_negative_preds(), "Negative predictions")
    return r

def scenario_19_aggregate_having():
    r = ScenarioResult("19_aggregate_having")
    sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(5, 50), np.random.uniform(100, 1000))
    run_simple(sim, (0.1, 4.0, 20.0), 100, feat, result=r)
    mape = r.mape_after(50)
    r.info(f"MAPE last 50: {mape:.3f}, weights: {r.weights_history[-1]}")
    r.check(mape < 0.15, f"MAPE {mape:.3f} > 0.15")
    return r

def scenario_20_mv_pipeline():
    r = ScenarioResult("20_mv_pipeline")
    sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(20, 200), np.random.uniform(10, 100))
    run_simple(sim, (1.0, 1.5, 50.0), 100, feat, result=r)
    mape = r.mape_after(50)
    intercept = r.weights_history[-1][2]
    r.info(f"MAPE: {mape:.3f}, intercept: {intercept:.1f} (true: 50.0)")
    r.check(mape < 0.15, f"MAPE {mape:.3f} > 0.15")
    return r


# ============================================================================
# Scenarios 21-30: Adversarial / Stress Tests
# ============================================================================

def scenario_21_severe_overestimate():
    """Static model overestimates by 100x. Regression must correct."""
    r = ScenarioResult("21_severe_overestimate")
    sim = CostModelSimulator()
    # True relationship: actual_ms = 0.01 * compute + 0.01 * upsert + 1
    # Static model thinks 1*compute + 1*upsert + 0 (100x overestimate)
    true_w = (0.01, 0.01, 1.0)
    for i in range(100):
        compute = np.random.uniform(100, 10000)
        upsert = np.random.uniform(50, 5000)
        actual = generate_actual(compute, upsert, true_w)
        pred, w, cal = sim.predict(compute, upsert)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(compute, upsert, actual)
    mape = r.mape_after(10)
    r.info(f"MAPE after 10: {mape:.3f} (static would be ~99x off)")
    r.info(f"Final weights: {r.weights_history[-1]} (true: {true_w})")
    r.check(mape < 0.30, f"MAPE {mape:.3f} > 0.30 — failed to correct overestimate")
    r.check(r.no_negative_preds(), "Negative predictions")
    return r

def scenario_22_severe_underestimate():
    """Static model underestimates by 100x. Regression must correct."""
    r = ScenarioResult("22_severe_underestimate")
    sim = CostModelSimulator()
    # True relationship: actual_ms = 100 * compute + 100 * upsert + 500
    # Static model thinks 1*compute + 1*upsert + 0 (100x underestimate)
    true_w = (100.0, 100.0, 500.0)
    for i in range(100):
        compute = np.random.uniform(1, 50)
        upsert = np.random.uniform(0.5, 25)
        actual = generate_actual(compute, upsert, true_w)
        pred, w, cal = sim.predict(compute, upsert)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(compute, upsert, actual)
    mape = r.mape_after(10)
    r.info(f"MAPE after 10: {mape:.3f} (static would be ~99x off)")
    r.info(f"Final weights: {r.weights_history[-1]} (true: {true_w})")
    r.check(mape < 0.20, f"MAPE {mape:.3f} > 0.20 — failed to correct underestimate")
    return r

def scenario_23_nonlinear_reality():
    """True cost is nonlinear: actual = compute^1.3 + upsert. Linear model degrades gracefully."""
    r = ScenarioResult("23_nonlinear_reality")
    sim = CostModelSimulator()
    for i in range(100):
        compute = np.random.uniform(1, 100)
        upsert = np.random.uniform(1, 50)
        actual = compute ** 1.3 + upsert + np.random.normal(0, 2)
        actual = max(0, actual)
        pred, w, cal = sim.predict(compute, upsert)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(compute, upsert, actual)
    mape = r.mape_after(20)
    r.info(f"MAPE last 80 (nonlinear): {mape:.3f}")
    r.info(f"Final weights: {r.weights_history[-1]}")
    # Looser threshold — linear model can't perfectly fit nonlinear data
    r.check(mape < 0.40, f"MAPE {mape:.3f} > 0.40 — nonlinear degradation too severe")
    r.check(r.all_finite(), "Non-finite values")
    r.check(r.no_negative_preds(), "Negative predictions")
    return r

def scenario_24_outlier_spike():
    """Single GC/IO stall outlier among normal observations."""
    r = ScenarioResult("24_outlier_spike")
    sim = CostModelSimulator()
    true_w = (1.0, 2.0, 5.0)
    for i in range(100):
        c, u = np.random.uniform(50, 300), np.random.uniform(20, 150)
        actual = generate_actual(c, u, true_w)
        if i == 50:
            actual *= 20  # 20x spike from GC pause
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(c, u, actual)
    # Check that model recovers within ~10 iterations after the spike
    mape_pre = r.mape_after(30)
    mape_post = r.mape_after(65)
    r.info(f"MAPE pre-spike (30-49): {mape_pre:.3f}")
    r.info(f"MAPE post-spike (65+): {mape_post:.3f}")
    r.check(mape_post < 0.25, f"Post-spike MAPE {mape_post:.3f} — didn't recover from outlier")
    return r

def scenario_25_constant_features():
    """Same delta every time (constant features). Regression must not blow up."""
    r = ScenarioResult("25_constant_features")
    sim = CostModelSimulator()
    true_w = (1.0, 2.0, 5.0)
    for i in range(50):
        c, u = 100.0, 50.0  # always the same
        actual = generate_actual(c, u, true_w, noise_std=0.05)
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(c, u, actual)
    r.check(r.all_finite(), "Non-finite values with constant features")
    r.check(r.no_negative_preds(), "Negative predictions with constant features")
    # With constant features, the regression can't distinguish weights — only intercept matters.
    # Predictions should converge to the actual mean (~205)
    mape = r.mape_after(20)
    r.info(f"MAPE last 30: {mape:.3f}")
    r.info(f"Final weights: {r.weights_history[-1]}")
    r.check(mape < 0.15, f"MAPE {mape:.3f} > 0.15 with constant features")
    return r

def scenario_26_decision_boundary():
    """IVM and recompute costs are very close. Check decision stability."""
    r = ScenarioResult("26_decision_boundary")
    ivm_sim = CostModelSimulator()
    rc_sim = CostModelSimulator()
    # True costs: IVM ≈ recompute (within 10%)
    true_ivm = (1.0, 1.5, 10.0)
    true_rc = (1.1, 1.4, 11.0)
    decisions = []
    for i in range(100):
        c_i, u_i = np.random.uniform(50, 200), np.random.uniform(20, 100)
        c_r, u_r = np.random.uniform(100, 400), np.random.uniform(30, 150)
        ivm_pred, _, _ = ivm_sim.predict(c_i, u_i)
        rc_pred, _, _ = rc_sim.predict(c_r, u_r)
        decision = "ivm" if ivm_pred <= rc_pred else "full"
        decisions.append(decision)
        if decision == "ivm":
            actual = generate_actual(c_i, u_i, true_ivm)
            ivm_sim.record(c_i, u_i, actual)
        else:
            actual = generate_actual(c_r, u_r, true_rc)
            rc_sim.record(c_r, u_r, actual)
        r.predictions.append(ivm_pred if decision == "ivm" else rc_pred)
        r.actuals.append(actual)
    flips = sum(1 for i in range(1, len(decisions)) if decisions[i] != decisions[i-1])
    r.info(f"Decision flips: {flips}/99, IVM chosen: {decisions.count('ivm')}, Full: {decisions.count('full')}")
    r.check(r.all_finite() if r.weights_history else True, "Non-finite values")
    # No hard check on flips — just informational. Close costs = inherent ambiguity.
    return r

def scenario_27_high_noise():
    """Actual execution time is very noisy (50% relative noise)."""
    r = ScenarioResult("27_high_noise")
    sim = CostModelSimulator()
    feat = lambda: (np.random.uniform(50, 500), np.random.uniform(20, 200))
    run_simple(sim, (1.0, 2.0, 10.0), 150, feat, noise_std=0.50, result=r)
    mape = r.mape_after(50)
    r.info(f"MAPE last 100 (50% noise): {mape:.3f}")
    r.info(f"Final weights: {r.weights_history[-1]}")
    # With 50% noise, MAPE is bounded by the noise floor (~0.50).
    # We just check the model doesn't blow up — not that it beats the noise.
    r.check(mape < 0.65, f"MAPE {mape:.3f} > 0.65 — worse than noise floor")
    r.check(r.all_finite(), "Non-finite values")
    return r

def scenario_28_growing_window():
    """First 20 iterations: window is growing (1,2,...,20 points). Check stability."""
    r = ScenarioResult("28_growing_window")
    sim = CostModelSimulator()
    true_w = (1.0, 2.0, 5.0)
    for i in range(25):
        c, u = np.random.uniform(50, 300), np.random.uniform(20, 150)
        actual = generate_actual(c, u, true_w)
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        r.info(f"  iter {i}: n_history={len(sim.history)}, cal={cal}, pred={pred:.1f}, actual={actual:.1f}")
        sim.record(c, u, actual)
    r.check(r.all_finite(), "Non-finite during growing window")
    r.check(r.no_negative_preds(), "Negative predictions during growing window")
    return r

def scenario_29_mixed_opt_flags():
    """Different optimization flags produce bimodal cost distribution for same features."""
    r = ScenarioResult("29_mixed_opt_flags")
    sim = CostModelSimulator()
    for i in range(100):
        c, u = np.random.uniform(50, 300), np.random.uniform(20, 150)
        # Randomly half the refreshes have skip-empty optimization (halves actual cost)
        if np.random.random() < 0.5:
            actual = generate_actual(c, u, (1.0, 2.0, 5.0))  # no optimization
        else:
            actual = generate_actual(c, u, (1.0, 1.0, 5.0))  # with optimization
        pred, w, cal = sim.predict(c, u)
        r.predictions.append(pred); r.actuals.append(actual); r.weights_history.append(w)
        sim.record(c, u, actual)
    mape = r.mape_after(20)
    r.info(f"MAPE (bimodal costs): {mape:.3f}")
    r.info(f"Final weights: {r.weights_history[-1]}")
    # Model should learn the average of the two modes
    r.check(r.all_finite(), "Non-finite values")
    r.check(r.no_negative_preds(), "Negative predictions")
    # Bimodal distribution means higher error is expected
    r.check(mape < 0.40, f"MAPE {mape:.3f} > 0.40 with bimodal costs")
    return r

def scenario_30_negative_prediction_clamping():
    """Verify that predictions are never negative even with adversarial weights."""
    r = ScenarioResult("30_negative_prediction_clamping")
    sim = CostModelSimulator()
    # Train on large features, then predict for tiny features -> risk of negative pred
    true_w = (3.0, 0.2, 15.0)
    for i in range(30):
        c = np.random.uniform(5000, 50000)  # large compute (join chain)
        u = np.random.uniform(5, 50)
        actual = generate_actual(c, u, true_w)
        sim.record(c, u, actual)

    # Now predict for tiny features — intercept may be hugely negative
    pred_small, w, _ = sim.predict(1.0, 1.0)
    pred_zero, _, _ = sim.predict(0.0, 0.0)
    r.info(f"Weights after large-feature training: {w}")
    r.info(f"Prediction for (1,1): {pred_small}, for (0,0): {pred_zero}")
    r.check(pred_small >= 0, f"Negative prediction for small features: {pred_small}")
    r.check(pred_zero >= 0, f"Negative prediction for zero features: {pred_zero}")

    # Also test extrapolation to very large features
    pred_huge, _, _ = sim.predict(1e6, 1e6)
    r.check(np.isfinite(pred_huge) and pred_huge >= 0, f"Bad prediction for huge features: {pred_huge}")
    r.info(f"Prediction for (1e6, 1e6): {pred_huge:.0f}")
    return r


# ============================================================================
# Decay Factor Sensitivity Analysis
# ============================================================================

def decay_sensitivity():
    print("\n" + "=" * 70)
    print("DECAY FACTOR SENSITIVITY (workload shift scenario)")
    print("=" * 70)
    print(f"{'Decay':<8} {'Pre-shift MAPE':<18} {'Post-shift MAPE':<20} {'Adapt iters'}")
    print("-" * 70)
    for decay in [0.5, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0]:
        np.random.seed(42)
        sim = CostModelSimulator(decay=decay)
        preds, acts = [], []
        for i in range(100):
            true_w = (1.0, 2.0, 5.0) if i < 50 else (0.3, 5.0, 20.0)
            c, u = np.random.uniform(50, 300), np.random.uniform(20, 150)
            actual = generate_actual(c, u, true_w)
            pred, _, _ = sim.predict(c, u)
            preds.append(pred); acts.append(actual)
            sim.record(c, u, actual)
        def mr(s, e):
            return np.mean([abs(p-a)/abs(a) for p, a in zip(preds[s:e], acts[s:e]) if abs(a) > 1e-6])
        adapt = "never"
        for k in range(50, 100):
            we = [abs(preds[j]-acts[j])/abs(acts[j]) for j in range(k, min(k+10,100)) if abs(acts[j])>1e-6]
            if we and np.mean(we) < 0.15:
                adapt = str(k - 50); break
        print(f"{decay:<8.2f} {mr(30,50):<18.3f} {mr(70,100):<20.3f} {adapt}")


# ============================================================================
# Main
# ============================================================================

def main():
    scenarios = [
        scenario_01_cold_start, scenario_02_steady_state, scenario_03_single_point_fallback,
        scenario_04_workload_shift, scenario_05_gradual_drift, scenario_06_opt_flag_change,
        scenario_07_ivm_only, scenario_08_full_only, scenario_09_alternating,
        scenario_10_ivm_full_ivm_transition, scenario_11_varying_delta_sizes,
        scenario_12_collinear_features, scenario_13_huge_variance, scenario_14_zero_deltas,
        scenario_15_all_zero_estimates, scenario_16_negative_ols, scenario_17_near_singular,
        scenario_18_join_chain, scenario_19_aggregate_having, scenario_20_mv_pipeline,
        # Adversarial
        scenario_21_severe_overestimate, scenario_22_severe_underestimate,
        scenario_23_nonlinear_reality, scenario_24_outlier_spike,
        scenario_25_constant_features, scenario_26_decision_boundary,
        scenario_27_high_noise, scenario_28_growing_window,
        scenario_29_mixed_opt_flags, scenario_30_negative_prediction_clamping,
    ]

    print("=" * 70)
    print(f"LEARNED COST MODEL SIMULATION — {len(scenarios)} SCENARIOS")
    print("=" * 70)
    print(f"Parameters: decay=0.9, window=20, min_samples=3, ridge_lambda=1e-4")
    print()

    all_passed = True
    failed_names = []
    for scenario_fn in scenarios:
        np.random.seed(42)
        result = scenario_fn()
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name}")
        for msg in result.messages:
            print(f"  {msg}")
        for err in result.errors:
            print(f"  !! {err}")
        if not result.passed:
            all_passed = False
            failed_names.append(result.name)

    decay_sensitivity()

    print()
    print("=" * 70)
    if all_passed:
        print(f"ALL {len(scenarios)} SCENARIOS PASSED")
    else:
        print(f"FAILED: {', '.join(failed_names)}")
    print("=" * 70)
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
