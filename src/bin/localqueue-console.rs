use eframe::egui::{self, Color32, RichText};
use localqueue::admin::{
    AdminStore, DatabaseInfo, DeliveryCounts, ExecutionSummary, FailedDelivery, FailureDetail,
    Page, SubscriptionConfig, SubscriptionSummary,
};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const PAGE_SIZE: u64 = 25;
const BACKGROUND: Color32 = Color32::from_rgb(3, 13, 23);
const SURFACE: Color32 = Color32::from_rgb(7, 22, 36);
const SURFACE_ALT: Color32 = Color32::from_rgb(9, 29, 46);
const BORDER: Color32 = Color32::from_rgb(27, 48, 67);
const TEXT: Color32 = Color32::from_rgb(231, 237, 245);
const MUTED: Color32 = Color32::from_rgb(161, 177, 196);
const BLUE: Color32 = Color32::from_rgb(58, 132, 255);
const GREEN: Color32 = Color32::from_rgb(45, 212, 146);
const YELLOW: Color32 = Color32::from_rgb(251, 191, 36);
const RED: Color32 = Color32::from_rgb(248, 92, 100);

#[derive(Clone, Default)]
struct ConsoleSnapshot {
    database: Option<DatabaseInfo>,
    subscriptions: Vec<SubscriptionSummary>,
    selected_queue: Option<String>,
    config: Option<SubscriptionConfig>,
    failures: Option<Page<FailedDelivery>>,
    executions: Option<Page<ExecutionSummary>>,
    failure_detail: Option<FailureDetail>,
    throughput: Vec<f64>,
    error: Option<String>,
}

enum Command {
    SelectQueue(String),
    FailurePage(u64),
    InspectFailure(i64),
    Retry(i64),
    Open(PathBuf),
}

struct Throughput {
    samples: VecDeque<(Instant, i64)>,
}
impl Throughput {
    fn push(&mut self, at: Instant, acknowledged: i64) {
        self.samples.push_back((at, acknowledged));
        while self
            .samples
            .front()
            .is_some_and(|(time, _)| at.duration_since(*time) > Duration::from_secs(60))
        {
            self.samples.pop_front();
        }
    }
    fn values(&self) -> Vec<f64> {
        self.samples
            .iter()
            .zip(self.samples.iter().skip(1))
            .map(|(before, after)| {
                let seconds = after.0.duration_since(before.0).as_secs_f64();
                if seconds > 0.0 {
                    (after.1 - before.1).max(0) as f64 / seconds
                } else {
                    0.0
                }
            })
            .collect()
    }
}

fn start_sampler(
    path: PathBuf,
    refresh_ms: Arc<AtomicU64>,
) -> (mpsc::Sender<Command>, Arc<Mutex<ConsoleSnapshot>>) {
    let (tx, rx) = mpsc::channel();
    let snapshot = Arc::new(Mutex::new(ConsoleSnapshot::default()));
    let destination = snapshot.clone();
    thread::spawn(move || {
        let mut store = AdminStore::open(path).ok();
        let mut selected_queue = None;
        let mut failure_offset = 0;
        let mut selected_failure = None;
        let mut throughput = Throughput {
            samples: VecDeque::new(),
        };
        loop {
            let timeout =
                Duration::from_millis(refresh_ms.load(Ordering::Relaxed).clamp(500, 1_000));
            match rx.recv_timeout(timeout) {
                Ok(Command::SelectQueue(queue)) => selected_queue = Some(queue),
                Ok(Command::FailurePage(offset)) => failure_offset = offset,
                Ok(Command::InspectFailure(id)) => selected_failure = Some(id),
                Ok(Command::Retry(id)) => {
                    if let Some(store) = &store {
                        if let Err(error) = store.retry_failed(id) {
                            let current = destination.lock().unwrap().clone();
                            *destination.lock().unwrap() = ConsoleSnapshot {
                                error: Some(error.to_string()),
                                ..current
                            };
                        }
                    }
                }
                Ok(Command::Open(path)) => {
                    store = AdminStore::open(path).ok();
                    selected_queue = None;
                    failure_offset = 0;
                    selected_failure = None;
                    throughput = Throughput {
                        samples: VecDeque::new(),
                    };
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            let next = match &store {
                Some(store) => sample(
                    store,
                    &mut selected_queue,
                    failure_offset,
                    selected_failure,
                    &mut throughput,
                ),
                None => ConsoleSnapshot {
                    error: Some("Unable to open LocalQueue database".to_owned()),
                    ..ConsoleSnapshot::default()
                },
            };
            *destination.lock().unwrap() = next;
        }
    });
    (tx, snapshot)
}

fn sample(
    store: &AdminStore,
    selected_queue: &mut Option<String>,
    failure_offset: u64,
    selected_failure: Option<i64>,
    throughput: &mut Throughput,
) -> ConsoleSnapshot {
    let subscriptions = match store.subscriptions() {
        Ok(value) => value,
        Err(error) => {
            return ConsoleSnapshot {
                error: Some(error.to_string()),
                ..ConsoleSnapshot::default()
            }
        }
    };
    if selected_queue.as_ref().is_none_or(|queue| {
        !subscriptions
            .iter()
            .any(|subscription| &subscription.queue == queue)
    }) {
        *selected_queue = subscriptions.first().map(|item| item.queue.clone());
    }
    let config = selected_queue
        .as_deref()
        .and_then(|queue| store.subscription_config(queue).ok().flatten());
    let acknowledged = selected_queue
        .as_deref()
        .and_then(|queue| subscriptions.iter().find(|item| item.queue == queue))
        .map_or(0, |item| item.counts.acknowledged);
    throughput.push(Instant::now(), acknowledged);
    ConsoleSnapshot {
        database: store.database_info().ok(),
        subscriptions,
        selected_queue: selected_queue.clone(),
        config,
        failures: store.failed_deliveries(failure_offset, PAGE_SIZE).ok(),
        executions: store.executions(0, PAGE_SIZE).ok(),
        failure_detail: selected_failure.and_then(|id| store.failure_detail(id).ok().flatten()),
        throughput: throughput.values(),
        error: None,
    }
}

struct ConsoleApp {
    commands: mpsc::Sender<Command>,
    snapshot: Arc<Mutex<ConsoleSnapshot>>,
    refresh_ms: Arc<AtomicU64>,
    refresh: u64,
    page: usize,
    path_input: String,
}
impl ConsoleApp {
    fn counts(snapshot: &ConsoleSnapshot) -> DeliveryCounts {
        snapshot
            .selected_queue
            .as_ref()
            .and_then(|queue| {
                snapshot
                    .subscriptions
                    .iter()
                    .find(|item| &item.queue == queue)
            })
            .map(|item| item.counts.clone())
            .unwrap_or_default()
    }
}
impl eframe::App for ConsoleApp {
    fn update(&mut self, ctx: &egui::Context, _: &mut eframe::Frame) {
        apply_console_theme(ctx);
        ctx.request_repaint_after(Duration::from_millis(100));
        let snapshot = self.snapshot.lock().unwrap().clone();
        egui::TopBottomPanel::top("top")
            .frame(panel_frame())
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("PATH").size(12.0).color(MUTED));
                    ui.add_sized(
                        [275.0, 32.0],
                        egui::TextEdit::singleline(&mut self.path_input).text_color(BLUE),
                    );
                    if ui.button("Open").clicked() {
                        let _ = self
                            .commands
                            .send(Command::Open(PathBuf::from(self.path_input.trim())));
                    }
                    ui.separator();
                    ui.label(
                        snapshot
                            .database
                            .as_ref()
                            .map_or("Opening database...".to_owned(), |info| {
                                info.path.display().to_string()
                            }),
                    );
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(RichText::new("Settings").color(MUTED));
                        ui.label(RichText::new("Theme").color(MUTED));
                        egui::ComboBox::from_id_salt("refresh")
                            .width(128.0)
                            .selected_text(format!("Refresh: {} s", self.refresh / 1_000))
                            .show_ui(ui, |ui| {
                                for ms in [500, 1_000] {
                                    if ui
                                        .selectable_value(
                                            &mut self.refresh,
                                            ms,
                                            format!("{} ms", ms),
                                        )
                                        .changed()
                                    {
                                        self.refresh_ms.store(ms, Ordering::Relaxed);
                                    }
                                }
                            });
                        ui.label(RichText::new("LIVE").color(GREEN));
                    });
                });
            });
        egui::SidePanel::left("sidebar")
            .resizable(false)
            .default_width(252.0)
            .frame(sidebar_frame())
            .show(ctx, |ui| {
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    egui::Frame::new()
                        .fill(Color32::from_rgb(19, 53, 110))
                        .corner_radius(8.0)
                        .inner_margin(egui::Margin::same(10))
                        .show(ui, |ui| {
                            ui.label(RichText::new("LQ").size(16.0).color(BLUE));
                        });
                    ui.vertical(|ui| {
                        ui.label(RichText::new("LOCALQUEUE").strong().size(16.0));
                        ui.label(RichText::new("Console v0.1.0").small().color(MUTED));
                    });
                });
                ui.add_space(28.0);
                for (index, label) in ["Overview", "Subscriptions", "Executions", "Failures"]
                    .iter()
                    .enumerate()
                {
                    if ui
                        .add_sized(
                            [218.0, 42.0],
                            egui::Button::new(*label).selected(self.page == index),
                        )
                        .clicked()
                    {
                        self.page = index;
                    }
                }
                ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                    ui.separator();
                    if let Some(database) = &snapshot.database {
                        ui.label(RichText::new(format_size(database.size_bytes)).color(MUTED));
                        ui.label(RichText::new("Size").small().color(MUTED));
                        ui.add_space(6.0);
                        ui.label(
                            RichText::new(database.path.display().to_string())
                                .small()
                                .color(MUTED),
                        );
                        ui.label(RichText::new("File").small().color(MUTED));
                        ui.add_space(6.0);
                        ui.label(
                            RichText::new(format!(
                                "SQLite ({})",
                                database.journal_mode.to_uppercase()
                            ))
                            .color(MUTED),
                        );
                        ui.label(RichText::new("Connected to").small().color(MUTED));
                    }
                });
            });
        egui::CentralPanel::default()
            .frame(
                egui::Frame::new()
                    .fill(BACKGROUND)
                    .inner_margin(egui::Margin::same(18)),
            )
            .show(ctx, |ui| {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    if let Some(error) = &snapshot.error {
                        ui.colored_label(Color32::RED, error);
                    }
                    match self.page {
                        0 => {
                            if overview(ui, &snapshot, &self.commands) {
                                self.page = 3;
                            }
                        }
                        1 => subscriptions(ui, &snapshot, &self.commands),
                        2 => executions(ui, &snapshot),
                        _ => failures(ui, &snapshot, &self.commands),
                    }
                });
            });
    }
}

fn overview(
    ui: &mut egui::Ui,
    snapshot: &ConsoleSnapshot,
    commands: &mpsc::Sender<Command>,
) -> bool {
    let counts = ConsoleApp::counts(snapshot);
    let mut view_failures = false;
    ui.horizontal(|ui| {
        ui.vertical(|ui| {
            ui.heading(
                snapshot
                    .selected_queue
                    .as_deref()
                    .unwrap_or("No subscription"),
            );
            ui.label(RichText::new("Subscription | Last refreshed just now").color(MUTED));
        });
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            view_failures = ui
                .add_sized([152.0, 36.0], egui::Button::new("View failures"))
                .clicked();
            ui.add_enabled(false, egui::Button::new("Inspect config"));
            ui.label(status_badge(
                if counts.processing > 0 {
                    "ACTIVE"
                } else {
                    "IDLE"
                },
                if counts.processing > 0 { GREEN } else { MUTED },
            ));
        });
    });
    ui.add_space(16.0);
    let metric_width = ((ui.available_width() - 36.0) / 4.0).max(160.0);
    ui.horizontal(|ui| {
        ui.allocate_ui_with_layout(
            egui::vec2(metric_width, 122.0),
            egui::Layout::top_down(egui::Align::LEFT),
            |ui| metric(ui, "READY", counts.ready, "Queued deliveries", BLUE),
        );
        ui.allocate_ui_with_layout(
            egui::vec2(metric_width, 122.0),
            egui::Layout::top_down(egui::Align::LEFT),
            |ui| metric(ui, "PROCESSING", counts.processing, "Leases active", YELLOW),
        );
        ui.allocate_ui_with_layout(
            egui::vec2(metric_width, 122.0),
            egui::Layout::top_down(egui::Align::LEFT),
            |ui| {
                metric(
                    ui,
                    "ACKNOWLEDGED",
                    counts.acknowledged,
                    "Completed deliveries",
                    GREEN,
                )
            },
        );
        ui.allocate_ui_with_layout(
            egui::vec2(metric_width, 122.0),
            egui::Layout::top_down(egui::Align::LEFT),
            |ui| metric(ui, "FAILED", counts.failed, "Needs review", RED),
        );
    });
    ui.add_space(18.0);
    let charts_width = ui.available_width();
    let graph_width = (charts_width * 0.64 - 6.0).max(320.0);
    let diagnostics_width = (charts_width - graph_width - 12.0).max(230.0);
    ui.horizontal(|ui| {
        ui.allocate_ui_with_layout(
            egui::vec2(graph_width, 360.0),
            egui::Layout::top_down(egui::Align::LEFT),
            |ui| throughput_card(ui, &snapshot.throughput),
        );
        ui.allocate_ui_with_layout(
            egui::vec2(diagnostics_width, 360.0),
            egui::Layout::top_down(egui::Align::LEFT),
            |ui| diagnostics_card(ui, snapshot),
        );
    });
    ui.add_space(14.0);
    recent_failures_card(ui, snapshot, commands);
    view_failures
}
fn metric(ui: &mut egui::Ui, label: &str, value: i64, detail: &str, color: Color32) {
    card().show(ui, |ui| {
        ui.horizontal(|ui| {
            ui.label(RichText::new(label).size(13.0).color(color));
        });
        ui.add_space(10.0);
        ui.label(RichText::new(format_count(value)).size(30.0));
        ui.add_space(8.0);
        ui.label(RichText::new(detail).small().color(MUTED));
    });
}

fn apply_console_theme(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();
    visuals.panel_fill = BACKGROUND;
    visuals.window_fill = SURFACE;
    visuals.extreme_bg_color = BACKGROUND;
    visuals.faint_bg_color = SURFACE_ALT;
    visuals.widgets.inactive.bg_fill = SURFACE_ALT;
    visuals.widgets.inactive.fg_stroke.color = TEXT;
    visuals.widgets.hovered.bg_fill = Color32::from_rgb(16, 47, 84);
    visuals.widgets.active.bg_fill = Color32::from_rgb(21, 65, 129);
    visuals.selection.bg_fill = Color32::from_rgb(27, 73, 143);
    visuals.window_stroke = egui::Stroke::new(1.0, BORDER);
    ctx.set_visuals(visuals);
}

fn panel_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(BACKGROUND)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .inner_margin(egui::Margin::symmetric(28, 16))
}
fn sidebar_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(Color32::from_rgb(2, 14, 26))
        .stroke(egui::Stroke::new(1.0, BORDER))
        .inner_margin(egui::Margin::symmetric(18, 20))
}
fn card() -> egui::Frame {
    egui::Frame::new()
        .fill(SURFACE)
        .stroke(egui::Stroke::new(1.0, BORDER))
        .corner_radius(7.0)
        .inner_margin(egui::Margin::same(18))
}
fn format_count(value: i64) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let chunks = digits
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_default();
    format!("{}{}", if negative { "-" } else { "" }, chunks.join(","))
}
fn format_size(bytes: Option<u64>) -> String {
    match bytes {
        Some(bytes) if bytes >= 1_000_000 => format!("{:.1} MB", bytes as f64 / 1_000_000.0),
        Some(bytes) if bytes >= 1_000 => format!("{:.1} KB", bytes as f64 / 1_000.0),
        Some(bytes) => format!("{bytes} B"),
        None => "Unknown".into(),
    }
}
fn status_badge(label: &str, color: Color32) -> RichText {
    RichText::new(format!("  {label}  "))
        .small()
        .strong()
        .color(color)
        .background_color(Color32::from_rgba_unmultiplied(
            color.r(),
            color.g(),
            color.b(),
            32,
        ))
}

fn throughput_card(ui: &mut egui::Ui, values: &[f64]) {
    card().show(ui, |ui| {
        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.label(RichText::new("THROUGHPUT").strong());
                ui.label(
                    RichText::new("ACK/s | last 60 seconds")
                        .small()
                        .color(MUTED),
                );
            });
            ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                let peak = values.iter().copied().fold(0.0, f64::max);
                let current = values.last().copied().unwrap_or_default();
                ui.vertical(|ui| {
                    ui.label(RichText::new(format!("{peak:.0} /s")).size(16.0));
                    ui.label(RichText::new("Peak").small().color(MUTED));
                });
                ui.add_space(20.0);
                ui.vertical(|ui| {
                    ui.label(
                        RichText::new(format!("{current:.0} /s"))
                            .size(16.0)
                            .color(BLUE),
                    );
                    ui.label(RichText::new("Current").small().color(MUTED));
                });
            });
        });
        ui.add_space(16.0);
        let (rect, _) = ui.allocate_exact_size(
            egui::vec2(ui.available_width(), 220.0),
            egui::Sense::hover(),
        );
        let painter = ui.painter_at(rect);
        for part in 0..5 {
            let y = rect.top() + rect.height() * part as f32 / 4.0;
            painter.line_segment(
                [egui::pos2(rect.left(), y), egui::pos2(rect.right(), y)],
                egui::Stroke::new(1.0, Color32::from_rgb(16, 42, 64)),
            );
        }
        if values.len() > 1 {
            let max = values.iter().copied().fold(1.0_f64, f64::max) as f32;
            let points: Vec<_> = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    egui::pos2(
                        rect.left() + rect.width() * index as f32 / (values.len() - 1) as f32,
                        rect.bottom() - rect.height() * (*value as f32 / max),
                    )
                })
                .collect();
            painter.line(points, egui::Stroke::new(2.0, BLUE));
        } else {
            painter.text(
                rect.center(),
                egui::Align2::CENTER_CENTER,
                "Collecting acknowledgement samples...",
                egui::FontId::proportional(13.0),
                MUTED,
            );
        }
    });
}

fn diagnostics_card(ui: &mut egui::Ui, snapshot: &ConsoleSnapshot) {
    card().show(ui, |ui| {
        ui.label(RichText::new("DIAGNOSTICS").strong());
        ui.add_space(10.0);
        if let Some(config) = &snapshot.config {
            diagnostic_row(ui, "Active leases", &config.active_leases.to_string());
            diagnostic_row(
                ui,
                "Retry ceiling",
                &format!("{} attempts", config.max_max_attempts),
            );
            diagnostic_row(ui, "Concurrency", "Runtime-only");
            diagnostic_row(ui, "Lease duration", "Runtime-only");
            diagnostic_row(
                ui,
                "Journal mode",
                snapshot
                    .database
                    .as_ref()
                    .map(|item| item.journal_mode.as_str())
                    .unwrap_or("Unknown"),
            );
            diagnostic_row(
                ui,
                "Database size",
                &format_size(snapshot.database.as_ref().and_then(|item| item.size_bytes)),
            );
        } else {
            ui.label(RichText::new("No queue-owned configuration available.").color(MUTED));
        }
        ui.add_space(8.0);
        ui.label(
            RichText::new("Runtime policy is intentionally not guessed from the database.")
                .small()
                .color(MUTED),
        );
    });
}
fn diagnostic_row(ui: &mut egui::Ui, label: &str, value: &str) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.label(RichText::new(value).color(TEXT))
        });
    });
    ui.separator();
}

fn recent_failures_card(
    ui: &mut egui::Ui,
    snapshot: &ConsoleSnapshot,
    commands: &mpsc::Sender<Command>,
) {
    card().show(ui, |ui| {
        ui.horizontal(|ui| {
            ui.label(RichText::new("RECENT FAILURES").strong());
            let total = snapshot.failures.as_ref().map_or(0, |page| page.total);
            ui.label(status_badge(&total.to_string(), RED));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.label(
                    RichText::new("Use Failures for full pagination")
                        .small()
                        .color(MUTED),
                )
            });
        });
        ui.add_space(8.0);
        egui::Grid::new("overview_failures")
            .striped(true)
            .min_col_width(90.0)
            .show(ui, |ui| {
                for header in [
                    "Delivery",
                    "Subscription",
                    "Category / error",
                    "Attempts",
                    "Actions",
                ] {
                    ui.label(RichText::new(header).small().color(MUTED));
                }
                ui.end_row();
                if let Some(page) = &snapshot.failures {
                    for failure in page.items.iter().take(5) {
                        ui.label(RichText::new(format!("#{}", failure.id)).color(BLUE));
                        ui.label(&failure.queue);
                        ui.label(
                            failure
                                .failure_category
                                .as_deref()
                                .or(failure.failure_reason.as_deref())
                                .or(failure.last_error.as_deref())
                                .unwrap_or("Unknown"),
                        );
                        ui.label(format!("{}/{}", failure.attempts, failure.max_attempts));
                        let inspect = ui
                            .small_button("Inspect")
                            .on_hover_text("Inspect failure")
                            .clicked();
                        let retry = ui
                            .small_button("Retry")
                            .on_hover_text("Retry delivery")
                            .clicked();
                        if inspect {
                            let _ = commands.send(Command::InspectFailure(failure.id));
                        }
                        if retry {
                            let _ = commands.send(Command::Retry(failure.id));
                        }
                        ui.end_row();
                    }
                }
            });
    });
}
fn subscriptions(ui: &mut egui::Ui, snapshot: &ConsoleSnapshot, commands: &mpsc::Sender<Command>) {
    ui.heading("Subscriptions");
    for subscription in &snapshot.subscriptions {
        if ui
            .selectable_label(
                snapshot.selected_queue.as_deref() == Some(&subscription.queue),
                format!(
                    "{} | {} ready / {} failed",
                    subscription.queue, subscription.counts.ready, subscription.counts.failed
                ),
            )
            .clicked()
        {
            let _ = commands.send(Command::SelectQueue(subscription.queue.clone()));
        }
    }
}
fn executions(ui: &mut egui::Ui, snapshot: &ConsoleSnapshot) {
    ui.heading("Executions");
    egui::Grid::new("executions").striped(true).show(ui, |ui| {
        ui.label("Execution");
        ui.label("Source");
        ui.label("State");
        ui.label("Deliveries");
        ui.end_row();
        if let Some(page) = &snapshot.executions {
            for execution in &page.items {
                ui.label(&execution.execution_id);
                ui.label(&execution.source_name);
                ui.label(if execution.completed_at.is_some() {
                    "Complete"
                } else {
                    "Active"
                });
                ui.label(format!(
                    "{} acked / {} failed",
                    execution.counts.acknowledged, execution.counts.failed
                ));
                ui.end_row();
            }
        }
    });
}
fn failures(ui: &mut egui::Ui, snapshot: &ConsoleSnapshot, commands: &mpsc::Sender<Command>) {
    ui.heading("Recent failures");
    egui::Grid::new("failures").striped(true).show(ui, |ui| {
        ui.label("Delivery");
        ui.label("Subscription");
        ui.label("Reason");
        ui.label("Actions");
        ui.end_row();
        if let Some(page) = &snapshot.failures {
            for failure in &page.items {
                ui.label(failure.id.to_string());
                ui.label(&failure.queue);
                ui.label(
                    failure
                        .failure_reason
                        .as_deref()
                        .or(failure.last_error.as_deref())
                        .unwrap_or("Unknown"),
                );
                let inspect = ui.button("Inspect").clicked();
                let retry = ui.button("Retry").clicked();
                if inspect {
                    let _ = commands.send(Command::InspectFailure(failure.id));
                }
                if retry {
                    let _ = commands.send(Command::Retry(failure.id));
                }
                ui.end_row();
            }
        }
    });
    if let Some(page) = &snapshot.failures {
        ui.horizontal(|ui| {
            let previous = ui
                .add_enabled(page.offset > 0, egui::Button::new("Previous"))
                .clicked();
            let next = ui
                .add_enabled(
                    page.offset + page.limit < page.total as u64,
                    egui::Button::new("Next"),
                )
                .clicked();
            if previous {
                let _ = commands.send(Command::FailurePage(page.offset.saturating_sub(PAGE_SIZE)));
            }
            if next {
                let _ = commands.send(Command::FailurePage(page.offset + PAGE_SIZE));
            }
            ui.label(format!("{} failures", page.total));
        });
    }
    if let Some(detail) = &snapshot.failure_detail {
        ui.separator();
        ui.heading(format!("Failed delivery #{}", detail.delivery.id));
        ui.label(
            detail
                .delivery
                .last_error
                .as_deref()
                .unwrap_or("No error text stored"),
        );
        let mut payload = String::from_utf8_lossy(&detail.payload).into_owned();
        if ui.button("Copy payload").clicked() {
            ui.ctx().copy_text(payload.clone());
        }
        ui.add(
            egui::TextEdit::multiline(&mut payload)
                .desired_rows(6)
                .interactive(false),
        );
    }
}

fn main() -> eframe::Result<()> {
    let path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!("usage: localqueue-console /data/contacts");
            std::process::exit(2)
        });
    let refresh_ms = Arc::new(AtomicU64::new(1_000));
    let (commands, snapshot) = start_sampler(path.clone(), refresh_ms.clone());
    eframe::run_native(
        "LocalQueue Console",
        eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default().with_inner_size([1536.0, 1024.0]),
            ..Default::default()
        },
        Box::new(move |_| {
            Ok(Box::new(ConsoleApp {
                commands,
                snapshot,
                refresh_ms,
                refresh: 1_000,
                page: 0,
                path_input: path.display().to_string(),
            }))
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn throughput_uses_acknowledgement_delta_per_second() {
        let start = Instant::now();
        let mut sampler = Throughput {
            samples: VecDeque::new(),
        };
        sampler.push(start, 10);
        sampler.push(start + Duration::from_secs(2), 16);
        assert_eq!(sampler.values(), vec![3.0]);
    }
    #[test]
    fn snapshot_replacement_is_atomic_from_the_ui_point_of_view() {
        let snapshot = Arc::new(Mutex::new(ConsoleSnapshot::default()));
        *snapshot.lock().unwrap() = ConsoleSnapshot {
            error: Some("new snapshot".into()),
            ..ConsoleSnapshot::default()
        };
        assert_eq!(
            snapshot.lock().unwrap().error.as_deref(),
            Some("new snapshot")
        );
    }
}
