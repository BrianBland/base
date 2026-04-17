//! Criterion benchmark for synthetic prefetch experiments.

use std::env;
use std::hint::black_box;
use std::time::Duration;

use alloy_primitives::Address;
use base_prefetch::{
    Erc20SwapLeg, PrefetchExperiment, PrefetchExperimentConfig, PrefetchMode, TxShape,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn prefetch_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefetch_synthetic");
    let miss_latencies_us = configured_miss_latencies_us();
    let prefetch_leads_us = [0_u64, 100];
    let planner_modes = [true, false];
    let tx_shapes = [TxShape::Transfer, TxShape::TransferFrom, TxShape::Swap];
    let modes = [PrefetchMode::Baseline, PrefetchMode::Synchronous, PrefetchMode::Asynchronous];

    for tx_shape in tx_shapes {
        for miss_latency_us in &miss_latencies_us {
            for prefetch_lead_us in prefetch_leads_us {
                for planner_enabled in planner_modes {
                    for mode in modes {
                        let id = BenchmarkId::new(
                            mode_name(mode),
                            format!(
                                "{}_miss={}us_lead={}us_planner={}",
                                shape_name(tx_shape),
                                miss_latency_us,
                                prefetch_lead_us,
                                if planner_enabled { "on" } else { "off" }
                            ),
                        );

                        group.bench_function(id, |b| {
                            let mut config = PrefetchExperimentConfig {
                                iterations: 1,
                                miss_latency: Duration::from_micros(*miss_latency_us),
                                execution_gap: Duration::from_micros(50),
                                prefetch_lead: Duration::from_micros(prefetch_lead_us),
                                use_prefetch_planner: planner_enabled,
                                ..Default::default()
                            };
                            config.context.tx_shape = tx_shape;
                            if tx_shape == TxShape::Swap {
                                config.swap_legs = sample_swap_legs();
                            }
                            let experiment = PrefetchExperiment::new(config);

                            b.iter(|| {
                                let elapsed = experiment.run_once(mode);
                                black_box(elapsed);
                            });
                        });
                    }
                }
            }
        }
    }

    group.finish();
}

fn deep_tree_prefetch_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("prefetch_synthetic_deep_tree");
    let miss_latencies_us = configured_miss_latencies_us();
    let prefetch_leads_us = [0_u64, 25, 100];
    let planner_modes = [true, false];
    let modes = [PrefetchMode::Baseline, PrefetchMode::Synchronous, PrefetchMode::Asynchronous];

    for miss_latency_us in &miss_latencies_us {
        for prefetch_lead_us in prefetch_leads_us {
            for planner_enabled in planner_modes {
                for mode in modes {
                    let id = BenchmarkId::new(
                        mode_name(mode),
                        format!(
                            "universal_router_2hop_miss={}us_lead={}us_planner={}",
                            miss_latency_us,
                            prefetch_lead_us,
                            if planner_enabled { "on" } else { "off" }
                        ),
                    );

                    group.bench_function(id, |b| {
                        let mut config =
                            PrefetchExperimentConfig::universal_router_two_hop_swap_like();
                        config.iterations = 1;
                        config.miss_latency = Duration::from_micros(*miss_latency_us);
                        config.execution_gap = Duration::from_micros(8);
                        config.prefetch_lead = Duration::from_micros(prefetch_lead_us);
                        config.use_prefetch_planner = planner_enabled;
                        let experiment = PrefetchExperiment::new(config);

                        b.iter(|| {
                            let elapsed = experiment.run_once(mode);
                            black_box(elapsed);
                        });
                    });
                }
            }
        }
    }

    group.finish();
}

const fn mode_name(mode: PrefetchMode) -> &'static str {
    match mode {
        PrefetchMode::Baseline => "baseline",
        PrefetchMode::Synchronous => "sync",
        PrefetchMode::Asynchronous => "async",
    }
}

const fn shape_name(shape: TxShape) -> &'static str {
    match shape {
        TxShape::Transfer => "transfer",
        TxShape::TransferFrom => "transfer_from",
        TxShape::Swap => "swap",
    }
}

fn sample_swap_legs() -> Vec<Erc20SwapLeg> {
    let pool = Address::with_last_byte(0x10);
    let router = Address::with_last_byte(0xF0);
    vec![
        Erc20SwapLeg {
            from: Address::with_last_byte(0x11),
            to: pool,
            allowance_spender: Some(router),
        },
        Erc20SwapLeg { from: pool, to: Address::with_last_byte(0x12), allowance_spender: None },
        Erc20SwapLeg {
            from: Address::with_last_byte(0x12),
            to: pool,
            allowance_spender: Some(router),
        },
        Erc20SwapLeg { from: pool, to: Address::with_last_byte(0x13), allowance_spender: None },
    ]
}

fn configured_miss_latencies_us() -> Vec<u64> {
    const ENV_NAME: &str = "BASE_PREFETCH_SIM_MISS_US";
    env::var(ENV_NAME).map_or_else(
        |_| vec![0_u64, 100, 500],
        |raw| {
            let parsed = raw
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .filter_map(|value| value.parse::<u64>().ok())
                .collect::<Vec<_>>();
            if parsed.is_empty() { vec![0_u64, 100, 500] } else { parsed }
        },
    )
}

criterion_group!(benches, prefetch_benches, deep_tree_prefetch_benches);
criterion_main!(benches);
