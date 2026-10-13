// ui/script.js
/**
 * ================================================================================
 * STUDENT PERFORMANCE ANALYSIS SYSTEM - ENTERPRISE CLIENT SCRIPT
 * ================================================================================
 * Architecture:
 * - Asynchronous REST API integration (Flask backend + Apache Spark / Pandas)
 * - Chart.js dynamic data visualization engine with PNG export
 * - Client-side high-performance pagination, multi-column search, and sort
 * - Dual-theme state manager (Light / Dark mode persistence via localStorage)
 * - Modern drag-and-drop CSV dataset uploader with progress tracking
 * ================================================================================
 */

// Backend URL pointing directly to the Flask server
const BACKEND_URL = 'http://127.0.0.1:5000';

// Backend state tracking and fallback dataset
let isBackendOnline = false;
let activeDataset = (typeof window !== 'undefined' && Array.isArray(window.STUDENT_DATASET) && window.STUDENT_DATASET.length > 0)
    ? [...window.STUDENT_DATASET]
    : [];

// High-speed client query cache for instantaneous 0ms tab switching
const CLIENT_QUERY_CACHE = {};

// Global state tracking
let currentActiveQuery = null;
let currentColumns = [];
let allRecords = [];
let filteredRecords = [];
let currentPage = 1;
let pageSize = 15;
let sortColumn = null;
let sortAscending = true;

// Chart.js instance tracking
let activeChart = null;
let currentChartData = null;
let currentChartType = 'bar';

// Query title and subtitle dictionary for breadcrumbs and headers
const QUERY_META = {
    'preview': { title: 'Dataset Preview', sub: 'First 15 sample records from loaded dataset' },
    'top10': { title: 'Top 10 Students', sub: 'Highest scoring students across all subjects' },
    'highest': { title: 'Subject Toppers', sub: 'Top ranking student in each individual course' },
    'lowest': { title: 'Lowest Scorers', sub: 'Lowest mark obtained in each course' },
    'subject_avg': { title: 'Subject Averages', sub: 'Course-level mean marks and student headcount' },
    'dept_avg': { title: 'Department Averages', sub: 'Mean academic performance grouped by branch' },
    'sem_perf': { title: 'Semester Trends', sub: 'Longitudinal progression of marks and attendance' },
    'pass_fail': { title: 'Pass / Fail Statistics', sub: 'Cohort breakdown based on 35% pass criterion' },
    'attendance': { title: 'Attendance Shortage (<75%)', sub: 'Students failing minimum KTU mandatory attendance quota' },
    'grade_dist': { title: 'Grade Distribution', sub: 'Letter grade frequency across the student body' },
    'ranking': { title: 'Student Rankings', sub: 'Comprehensive leaderboard sorted by aggregate overall average' },
    'class_stats': { title: 'Overall Class Statistics', sub: 'Executive cohort aggregate metrics' }
};

// Initialize Application on DOM Ready
document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    initSidebar();
    initSearchFilter();
    initDragAndDrop();
    checkStatus();

    // Auto-ping backend every 8s if currently offline to auto-switch to Spark when started
    setInterval(() => {
        if (!isBackendOnline) {
            fetch(`${BACKEND_URL}/status`)
                .then(res => res.ok ? res.json() : null)
                .then(data => {
                    if (data && data.status === "ready") {
                        isBackendOnline = true;
                        updateStatus("ready", `Active: ${data.filename} (${data.count.toLocaleString()} rows)`);
                        setEngineStatusText("Apache Spark local[*]");
                        showToast("Connected to Apache Spark JVM backend!", "success");
                        refreshKpiMetrics();
                        if (currentActiveQuery) runAnalysis(currentActiveQuery);
                    }
                })
                .catch(() => {});
        }
    }, 8000);
});

/**
 * ==============================================================================
 * THEME & UI CONTROLS
 * ==============================================================================
 */
function initTheme() {
    const savedTheme = localStorage.getItem('eduspark_theme') || 'light';
    document.documentElement.setAttribute('data-theme', savedTheme);

    const themeToggleBtn = document.getElementById('theme-toggle');
    if (themeToggleBtn) {
        themeToggleBtn.addEventListener('click', () => {
            const current = document.documentElement.getAttribute('data-theme') || 'light';
            const next = current === 'light' ? 'dark' : 'light';
            document.documentElement.setAttribute('data-theme', next);
            localStorage.setItem('eduspark_theme', next);

            // Re-render chart if present to match grid & text colors
            if (activeChart && currentChartData) {
                renderVisualChart(currentChartData);
            }
        });
    }
}

function initSidebar() {
    const sidebar = document.getElementById('sidebar');
    const openBtn = document.getElementById('sidebar-open-btn');
    const closeBtn = document.getElementById('sidebar-close-btn');

    if (openBtn && sidebar) {
        openBtn.addEventListener('click', () => sidebar.classList.add('open'));
    }
    if (closeBtn && sidebar) {
        closeBtn.addEventListener('click', () => sidebar.classList.remove('open'));
    }
}

function setActiveNavButton(id) {
    document.querySelectorAll('.nav-item').forEach(btn => btn.classList.remove('active'));
    const target = document.getElementById(id);
    if (target) target.classList.add('active');

    // Close sidebar on mobile after navigation
    const sidebar = document.getElementById('sidebar');
    if (sidebar && window.innerWidth < 992) {
        sidebar.classList.remove('open');
    }
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;

    let iconSvg = '';
    if (type === 'success') {
        iconSvg = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#10B981" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>';
    } else if (type === 'error') {
        iconSvg = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#EF4444" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>';
    } else {
        iconSvg = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#4F46E5" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';
    }

    toast.innerHTML = `${iconSvg}<span>${message}</span>`;
    container.appendChild(toast);

    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(100%)';
        toast.style.transition = 'all 0.3s ease';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

/**
 * ==============================================================================
 * ENGINE STATUS & HELPER UTILITIES
 * ==============================================================================
 */
function setEngineStatusText(text) {
    const el = document.getElementById('engine-status-text');
    if (el) el.textContent = text;
}

/**
 * ==============================================================================
 * CLIENT-SIDE ANALYTICS ENGINE (FALLBACK / STANDALONE MODE)
 * ==============================================================================
 */
function computeOfflineClassStats(records) {
    if (!records || records.length === 0) return [];
    const totalUniqueStudents = new Set(records.map(r => r.Student_ID)).size;
    const marksList = records.map(r => parseFloat(r.Marks) || 0);
    const attList = records.map(r => parseFloat(r.Attendance) || 0);
    const sumMarks = marksList.reduce((a, b) => a + b, 0);
    const sumAtt = attList.reduce((a, b) => a + b, 0);
    const avgMarks = marksList.length ? (sumMarks / marksList.length).toFixed(2) : 0;
    const maxMarks = marksList.length ? Math.max(...marksList) : 0;
    const minMarks = marksList.length ? Math.min(...marksList) : 0;
    const avgAtt = attList.length ? (sumAtt / attList.length).toFixed(2) : 0;
    const passCount = marksList.filter(m => m >= 35).length;
    const passPct = marksList.length ? ((passCount / marksList.length) * 100).toFixed(2) : 0;

    return [
        { Metric: "Total Unique Students", Value: totalUniqueStudents },
        { Metric: "Average Marks", Value: parseFloat(avgMarks) },
        { Metric: "Highest Mark Obtained", Value: maxMarks },
        { Metric: "Lowest Mark Obtained", Value: minMarks },
        { Metric: "Average Attendance (%)", Value: parseFloat(avgAtt) },
        { Metric: "Overall Pass Percentage (%)", Value: parseFloat(passPct) }
    ];
}

function computeOfflineAnalytics(queryType, records) {
    if (!records || records.length === 0) return { columns: [], data: [] };

    if (queryType === 'top10') {
        const sorted = [...records].sort((a, b) => (parseFloat(b.Marks) || 0) - (parseFloat(a.Marks) || 0));
        return {
            columns: ["Student_ID", "Student_Name", "Department", "Subject", "Marks"],
            data: sorted.slice(0, 10).map(r => ({
                Student_ID: r.Student_ID,
                Student_Name: r.Student_Name,
                Department: r.Department,
                Subject: r.Subject,
                Marks: parseFloat(r.Marks)
            }))
        };
    }
    if (queryType === 'highest') {
        const maxMarks = Math.max(...records.map(r => parseFloat(r.Marks) || 0));
        const matched = records.filter(r => (parseFloat(r.Marks) || 0) === maxMarks);
        return {
            columns: ["Student_ID", "Student_Name", "Department", "Subject", "Marks"],
            data: matched.map(r => ({
                Student_ID: r.Student_ID,
                Student_Name: r.Student_Name,
                Department: r.Department,
                Subject: r.Subject,
                Marks: parseFloat(r.Marks)
            }))
        };
    }
    if (queryType === 'lowest') {
        const minMarks = Math.min(...records.map(r => parseFloat(r.Marks) || 0));
        const matched = records.filter(r => (parseFloat(r.Marks) || 0) === minMarks);
        return {
            columns: ["Student_ID", "Student_Name", "Department", "Subject", "Marks"],
            data: matched.map(r => ({
                Student_ID: r.Student_ID,
                Student_Name: r.Student_Name,
                Department: r.Department,
                Subject: r.Subject,
                Marks: parseFloat(r.Marks)
            }))
        };
    }
    if (queryType === 'subject_avg') {
        const groups = {};
        records.forEach(r => {
            const sub = r.Subject || 'General';
            if (!groups[sub]) groups[sub] = [];
            groups[sub].push(parseFloat(r.Marks) || 0);
        });
        const data = Object.keys(groups).map(sub => ({
            Subject: sub,
            Average_Marks: parseFloat((groups[sub].reduce((a, b) => a + b, 0) / groups[sub].length).toFixed(2))
        })).sort((a, b) => b.Average_Marks - a.Average_Marks);
        return { columns: ["Subject", "Average_Marks"], data };
    }
    if (queryType === 'dept_avg') {
        const groups = {};
        records.forEach(r => {
            const dept = r.Department || 'General';
            if (!groups[dept]) groups[dept] = [];
            groups[dept].push(parseFloat(r.Marks) || 0);
        });
        const data = Object.keys(groups).map(dept => ({
            Department: dept,
            Average_Marks: parseFloat((groups[dept].reduce((a, b) => a + b, 0) / groups[dept].length).toFixed(2))
        })).sort((a, b) => b.Average_Marks - a.Average_Marks);
        return { columns: ["Department", "Average_Marks"], data };
    }
    if (queryType === 'sem_perf') {
        const groups = {};
        records.forEach(r => {
            const sem = r.Semester || '1';
            if (!groups[sem]) groups[sem] = { marks: [], att: [] };
            groups[sem].marks.push(parseFloat(r.Marks) || 0);
            groups[sem].att.push(parseFloat(r.Attendance) || 0);
        });
        const data = Object.keys(groups).sort((a, b) => parseInt(a) - parseInt(b)).map(sem => ({
            Semester: sem,
            Average_Marks: parseFloat((groups[sem].marks.reduce((a, b) => a + b, 0) / groups[sem].marks.length).toFixed(2)),
            Average_Attendance: parseFloat((groups[sem].att.reduce((a, b) => a + b, 0) / groups[sem].att.length).toFixed(2))
        }));
        return { columns: ["Semester", "Average_Marks", "Average_Attendance"], data };
    }
    if (queryType === 'pass_fail') {
        const total = records.length || 1;
        const passCount = records.filter(r => (parseFloat(r.Marks) || 0) >= 35).length;
        const failCount = total - passCount;
        return {
            columns: ["Status", "Count", "Percentage"],
            data: [
                { Status: "Pass", Count: passCount, Percentage: parseFloat(((passCount / total) * 100).toFixed(2)) },
                { Status: "Fail", Count: failCount, Percentage: parseFloat(((failCount / total) * 100).toFixed(2)) }
            ]
        };
    }
    if (queryType === 'attendance') {
        const shortage = records.filter(r => (parseFloat(r.Attendance) || 0) < 75.0)
            .sort((a, b) => (parseFloat(a.Attendance) || 0) - (parseFloat(b.Attendance) || 0));
        return {
            columns: ["Student_ID", "Student_Name", "Department", "Attendance"],
            data: shortage.map(r => ({
                Student_ID: r.Student_ID,
                Student_Name: r.Student_Name,
                Department: r.Department,
                Attendance: parseFloat(r.Attendance)
            }))
        };
    }
    if (queryType === 'grade_dist') {
        const groups = {};
        records.forEach(r => {
            const g = r.Grade || 'Unknown';
            groups[g] = (groups[g] || 0) + 1;
        });
        const data = Object.keys(groups).map(g => ({
            Grade: g,
            Student_Count: groups[g]
        })).sort((a, b) => b.Student_Count - a.Student_Count);
        return { columns: ["Grade", "Student_Count"], data };
    }
    if (queryType === 'ranking') {
        const studentMap = {};
        records.forEach(r => {
            const id = r.Student_ID;
            if (!studentMap[id]) studentMap[id] = { name: r.Student_Name, dept: r.Department, marks: [] };
            studentMap[id].marks.push(parseFloat(r.Marks) || 0);
        });
        const ranked = Object.keys(studentMap).map(id => {
            const arr = studentMap[id].marks;
            const avg = parseFloat((arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(2));
            return {
                Student_ID: id,
                Student_Name: studentMap[id].name,
                Department: studentMap[id].dept,
                Overall_Average: avg
            };
        }).sort((a, b) => b.Overall_Average - a.Overall_Average);

        ranked.forEach((r, idx) => { r.Rank = idx + 1; });
        return {
            columns: ["Rank", "Student_ID", "Student_Name", "Department", "Overall_Average"],
            data: ranked
        };
    }
    if (queryType === 'class_stats') {
        const stats = computeOfflineClassStats(records);
        return {
            columns: ["Metric", "Value"],
            data: stats
        };
    }
    return { columns: Object.keys(records[0] || {}), data: records };
}

/**
 * ==============================================================================
 * BACKEND & WORKFLOW CONTROLLERS
 * ==============================================================================
 */

/**
 * Checks server status and automatically loads default or active dataset.
 */
function checkStatus() {
    updateStatus("loading", "Verifying Spark status...");

    fetch(`${BACKEND_URL}/status`)
        .then(response => {
            if (!response.ok) throw new Error(`HTTP Error: ${response.status}`);
            return response.json();
        })
        .then(data => {
            if (data.status === "ready") {
                isBackendOnline = true;
                updateStatus("ready", `Active: ${data.filename} (${data.count.toLocaleString()} rows)`);
                setEngineStatusText("Apache Spark local[*]");
                enableControls(true);
                fetchPreview();
                refreshKpiMetrics();
                prefetchCommonQueries();
            } else {
                isBackendOnline = true;
                updateStatus("no_data", data.message || "No dataset loaded");
                enableControls(true);
                if (activeDataset.length > 0) {
                    loadOfflinePreview();
                    refreshKpiMetrics();
                    prefetchCommonQueries();
                }
            }
        })
        .catch(error => {
            isBackendOnline = false;
            if (activeDataset.length > 0) {
                updateStatus("offline", `Standalone Engine (${activeDataset.length.toLocaleString()} records)`);
                setEngineStatusText("Client In-Memory Engine");
                enableControls(true);
                loadOfflinePreview();
                refreshKpiMetrics();
                prefetchCommonQueries();
                showToast("Connected to In-Memory Dataset (10,000 records). Launch Flask for Apache Spark JVM.", "info");
            } else {
                updateStatus("error", "Spark Server Offline");
                enableControls(true);
                showToast("Backend server offline. Run 'manage_server.bat start' or check port 5000.", "error");
            }
        });
}

/**
 * Pre-fetches all reports in the background so every button click is instant (0ms).
 */
function prefetchCommonQueries() {
    const queries = ['top10', 'highest', 'lowest', 'subject_avg', 'dept_avg', 'sem_perf', 'pass_fail', 'attendance', 'grade_dist', 'ranking', 'class_stats'];
    queries.forEach(q => {
        if (!CLIENT_QUERY_CACHE[q]) {
            if (isBackendOnline) {
                fetch(`${BACKEND_URL}/analyze/${q}`)
                    .then(res => res.ok ? res.json() : null)
                    .then(data => {
                        if (data && !data.error) {
                            CLIENT_QUERY_CACHE[q] = data;
                        }
                    })
                    .catch(() => {});
            } else if (activeDataset.length > 0) {
                CLIENT_QUERY_CACHE[q] = computeOfflineAnalytics(q, activeDataset);
            }
        }
    });
}

/**
 * Fetches dataset preview rows from Spark driver or client dataset.
 */
function fetchPreview() {
    currentActiveQuery = null;
    setActiveNavButton('btn-preview');
    updateHeaderInfo('preview');
    document.getElementById('export-controls').style.display = 'none';
    hideChartSection();

    // Instant cache return
    if (CLIENT_QUERY_CACHE['preview']) {
        currentColumns = CLIENT_QUERY_CACHE['preview'].columns;
        allRecords = CLIENT_QUERY_CACHE['preview'].data;
        applyTableData(currentColumns, allRecords);
        return;
    }

    if (isBackendOnline) {
        showLoader("Loading initial DataFrame preview from Spark JVM...");
        fetch(`${BACKEND_URL}/preview`)
            .then(response => response.json())
            .then(data => {
                hideLoader();
                if (data.error) {
                    showErrorInTable(data.error);
                    return;
                }
                CLIENT_QUERY_CACHE['preview'] = data;
                currentColumns = data.columns;
                allRecords = data.data;
                applyTableData(data.columns, data.data);
            })
            .catch(error => {
                hideLoader();
                if (activeDataset.length > 0) {
                    loadOfflinePreview();
                } else {
                    showErrorInTable("Failed to load dataset preview: " + error.message);
                }
            });
    } else {
        loadOfflinePreview();
    }
}

function loadOfflinePreview() {
    hideLoader();
    if (!activeDataset || activeDataset.length === 0) {
        showErrorInTable("No dataset loaded. Drag and drop student.csv to begin.");
        return;
    }
    const cols = Object.keys(activeDataset[0]);
    currentColumns = cols;
    allRecords = activeDataset;
    CLIENT_QUERY_CACHE['preview'] = { columns: cols, data: activeDataset.slice(0, 15) };
    applyTableData(cols, activeDataset);
}

/**
 * Executes a chosen analytical query (via PySpark or client fallback).
 * Uses high-speed cache for instantaneous 0ms tab switching!
 */
function runAnalysis(queryType) {
    currentActiveQuery = queryType;
    setActiveNavButton(`btn-${queryType}`);
    updateHeaderInfo(queryType);

    // Show export button for analytical results
    document.getElementById('export-controls').style.display = 'inline-flex';

    // Instant cache return (0ms) - eliminates wait time completely
    if (CLIENT_QUERY_CACHE[queryType]) {
        renderAnalysisPayload(queryType, CLIENT_QUERY_CACHE[queryType]);
        return;
    }

    if (isBackendOnline) {
        showLoader(`Executing ${QUERY_META[queryType]?.title || queryType} query on Spark...`);

        fetch(`${BACKEND_URL}/analyze/${queryType}`)
            .then(response => {
                if (!response.ok) throw new Error(`HTTP error ${response.status}`);
                return response.json();
            })
            .then(data => {
                hideLoader();
                if (data.error) {
                    showErrorInTable(data.error);
                    showToast(data.error, "error");
                    return;
                }
                CLIENT_QUERY_CACHE[queryType] = data;
                renderAnalysisPayload(queryType, data);
            })
            .catch(error => {
                hideLoader();
                if (activeDataset.length > 0) {
                    executeOfflineAnalysis(queryType);
                } else {
                    showErrorInTable(`Analysis Error: ${error.message}`);
                    showToast(`Analysis request failed: ${error.message}`, "error");
                }
            });
    } else {
        executeOfflineAnalysis(queryType);
    }
}

function renderAnalysisPayload(queryType, payload) {
    hideLoader();
    currentColumns = payload.columns;
    allRecords = payload.data;
    applyTableData(payload.columns, payload.data);

    if (queryType === 'class_stats') {
        renderClassStatsCards(payload.data);
        hideChartSection();
    } else {
        document.getElementById('stats-display').style.display = 'none';
        prepareAndRenderChart(queryType, payload.data);
    }
}

function executeOfflineAnalysis(queryType) {
    hideLoader();
    if (!activeDataset || activeDataset.length === 0) {
        showErrorInTable("No records available to analyze. Please upload a student CSV file.");
        return;
    }
    const result = computeOfflineAnalytics(queryType, activeDataset);
    CLIENT_QUERY_CACHE[queryType] = result;
    renderAnalysisPayload(queryType, result);
}

/**
 * Triggers CSV export (via Spark server or client download).
 */
function exportReport() {
    if (!currentActiveQuery) {
        showToast("Please run an analysis query before exporting.", "error");
        return;
    }

    if (isBackendOnline) {
        const exportBtn = document.getElementById('btn-export');
        const originalText = exportBtn.innerHTML;
        exportBtn.disabled = true;
        exportBtn.innerHTML = `<span>Exporting...</span>`;

        fetch(`${BACKEND_URL}/export/${currentActiveQuery}`)
            .then(response => response.json())
            .then(data => {
                exportBtn.disabled = false;
                exportBtn.innerHTML = originalText;
                if (data.status === "success") {
                    showToast(`Report exported successfully! Saved to: ${data.path}`, "success");
                } else {
                    triggerClientCSVDownload(`${currentActiveQuery}_report.csv`, currentColumns, allRecords);
                }
            })
            .catch(() => {
                exportBtn.disabled = false;
                exportBtn.innerHTML = originalText;
                triggerClientCSVDownload(`${currentActiveQuery}_report.csv`, currentColumns, allRecords);
            });
    } else {
        triggerClientCSVDownload(`${currentActiveQuery}_report.csv`, currentColumns, allRecords);
    }
}

function triggerClientCSVDownload(filename, columns, data) {
    if (!data || data.length === 0) {
        showToast("No data to export.", "error");
        return;
    }
    const headerRow = columns.join(',');
    const rows = data.map(row => {
        return columns.map(c => {
            const val = row[c] === null || row[c] === undefined ? '' : String(row[c]);
            return val.includes(',') ? `"${val}"` : val;
        }).join(',');
    });
    const csvContent = [headerRow, ...rows].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    showToast(`Report "${filename}" downloaded successfully.`, "success");
}

/**
 * ==============================================================================
 * KPI SUMMARY CARDS LOADER
 * ==============================================================================
 */
function refreshKpiMetrics() {
    if (isBackendOnline) {
        fetch(`${BACKEND_URL}/analyze/class_stats`)
            .then(res => res.json())
            .then(data => {
                if (!data.data) return;
                const metrics = {};
                data.data.forEach(item => {
                    metrics[item.Metric] = item.Value;
                });

                animateValue('kpi-val-students', metrics["Total Unique Students"] || 0, false);
                animateValue('kpi-val-avg-marks', metrics["Average Marks"] || 0, true);
                animateValue('kpi-val-pass-rate', (metrics["Overall Pass Percentage (%)"] || 0) + '%', false);
                animateValue('kpi-val-attendance', (metrics["Average Attendance (%)"] || 0) + '%', false);
            })
            .catch(() => {
                if (activeDataset.length > 0) computeOfflineKpis();
            });

        // Also fetch attendance shortage count for KPI risk card
        fetch(`${BACKEND_URL}/analyze/attendance`)
            .then(res => res.json())
            .then(data => {
                if (data.data) {
                    animateValue('kpi-val-risk', data.data.length.toLocaleString(), false);
                }
            })
            .catch(() => {});
    } else if (activeDataset.length > 0) {
        computeOfflineKpis();
    }
}

function computeOfflineKpis() {
    const stats = computeOfflineClassStats(activeDataset);
    const metrics = {};
    stats.forEach(item => { metrics[item.Metric] = item.Value; });

    animateValue('kpi-val-students', metrics["Total Unique Students"] || 0, false);
    animateValue('kpi-val-avg-marks', metrics["Average Marks"] || 0, true);
    animateValue('kpi-val-pass-rate', (metrics["Overall Pass Percentage (%)"] || 0) + '%', false);
    animateValue('kpi-val-attendance', (metrics["Average Attendance (%)"] || 0) + '%', false);

    const shortageCount = activeDataset.filter(r => (parseFloat(r.Attendance) || 0) < 75.0).length;
    animateValue('kpi-val-risk', shortageCount.toLocaleString(), false);
}

function animateValue(elementId, endValue, isFloat = false) {
    const el = document.getElementById(elementId);
    if (!el) return;
    if (typeof endValue === 'number') {
        el.textContent = isFloat ? endValue.toFixed(2) : Math.round(endValue).toLocaleString();
    } else {
        el.textContent = endValue;
    }
}

/**
 * ==============================================================================
 * TABLE PAGINATION, SORTING & SEARCH ENGINE
 * ==============================================================================
 */
function initSearchFilter() {
    const searchInput = document.getElementById('table-search-input');
    const clearBtn = document.getElementById('search-clear-btn');

    if (!searchInput) return;

    searchInput.addEventListener('input', (e) => {
        const query = e.target.value.trim().toLowerCase();
        clearBtn.style.display = query.length > 0 ? 'block' : 'none';

        if (!query) {
            filteredRecords = [...allRecords];
        } else {
            filteredRecords = allRecords.filter(row => {
                return Object.values(row).some(val =>
                    String(val).toLowerCase().includes(query)
                );
            });
        }
        currentPage = 1;
        renderPaginatedView();
    });

    clearBtn.addEventListener('click', () => {
        searchInput.value = '';
        clearBtn.style.display = 'none';
        filteredRecords = [...allRecords];
        currentPage = 1;
        renderPaginatedView();
    });
}

function applyTableData(columns, records) {
    currentColumns = columns;
    allRecords = records || [];

    // Reset search box
    const searchInput = document.getElementById('table-search-input');
    if (searchInput) searchInput.value = '';
    const clearBtn = document.getElementById('search-clear-btn');
    if (clearBtn) clearBtn.style.display = 'none';

    filteredRecords = [...allRecords];
    currentPage = 1;
    sortColumn = null;

    renderTableHeader(columns);
    renderPaginatedView();
}

function renderTableHeader(columns) {
    const headerRow = document.getElementById('table-header');
    if (!headerRow) return;

    headerRow.innerHTML = '';
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = formatColumnTitle(col);
        th.title = `Click to sort by ${col}`;
        th.addEventListener('click', () => handleSort(col));
        headerRow.appendChild(th);
    });
}

function handleSort(col) {
    if (sortColumn === col) {
        sortAscending = !sortAscending;
    } else {
        sortColumn = col;
        sortAscending = true;
    }

    filteredRecords.sort((a, b) => {
        let valA = a[col];
        let valB = b[col];

        if (typeof valA === 'number' && typeof valB === 'number') {
            return sortAscending ? valA - valB : valB - valA;
        }
        return sortAscending
            ? String(valA).localeCompare(String(valB))
            : String(valB).localeCompare(String(valA));
    });

    renderPaginatedView();
}

function renderPaginatedView() {
    const tableBody = document.getElementById('table-body');
    const paginationFooter = document.getElementById('table-pagination');
    const recordCountText = document.getElementById('table-record-count');

    if (!tableBody) return;

    if (filteredRecords.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="${currentColumns.length || 1}" class="empty-state-cell">
                    <div class="empty-state">
                        <h4>No matching records</h4>
                        <p>No rows found matching your active filter criteria.</p>
                    </div>
                </td>
            </tr>
        `;
        if (paginationFooter) paginationFooter.style.display = 'none';
        if (recordCountText) recordCountText.textContent = '0 records found';
        return;
    }

    // Calculate pagination slices
    const totalRecords = filteredRecords.length;
    const totalPages = Math.ceil(totalRecords / pageSize);
    if (currentPage > totalPages) currentPage = totalPages;
    if (currentPage < 1) currentPage = 1;

    const startIdx = (currentPage - 1) * pageSize;
    const endIdx = Math.min(startIdx + pageSize, totalRecords);
    const pageRecords = filteredRecords.slice(startIdx, endIdx);

    // Build rows HTML
    let rowsHtml = '';
    pageRecords.forEach(row => {
        rowsHtml += '<tr>';
        currentColumns.forEach(col => {
            const val = row[col];
            rowsHtml += `<td>${renderCellContent(col, val)}</td>`;
        });
        rowsHtml += '</tr>';
    });

    tableBody.innerHTML = rowsHtml;

    // Update pagination controls
    if (paginationFooter) {
        paginationFooter.style.display = 'flex';
        document.getElementById('pagination-info').textContent =
            `Showing ${startIdx + 1} to ${endIdx} of ${totalRecords.toLocaleString()} entries`;

        const prevBtn = document.getElementById('prev-page-btn');
        const nextBtn = document.getElementById('next-page-btn');
        if (prevBtn) prevBtn.disabled = currentPage === 1;
        if (nextBtn) nextBtn.disabled = currentPage === totalPages;

        renderPageNumbers(totalPages);
    }

    if (recordCountText) {
        recordCountText.textContent = `${totalRecords.toLocaleString()} records`;
    }
}

function renderPageNumbers(totalPages) {
    const container = document.getElementById('page-numbers-container');
    if (!container) return;

    container.innerHTML = '';
    const maxButtons = 5;
    let startPage = Math.max(1, currentPage - 2);
    let endPage = Math.min(totalPages, startPage + maxButtons - 1);

    if (endPage - startPage < maxButtons - 1) {
        startPage = Math.max(1, endPage - maxButtons + 1);
    }

    for (let p = startPage; p <= endPage; p++) {
        const btn = document.createElement('button');
        btn.className = `page-num ${p === currentPage ? 'active' : ''}`;
        btn.textContent = p;
        btn.addEventListener('click', () => goToPage(p));
        container.appendChild(btn);
    }
}

function goToPage(page) {
    currentPage = page;
    renderPaginatedView();
}

function changePageSize(newSize) {
    pageSize = parseInt(newSize, 10);
    currentPage = 1;
    renderPaginatedView();
}

/**
 * Formats rich cell UI (badges, progress bars, medals).
 */
function renderCellContent(columnName, value) {
    if (value === null || value === undefined) return '<span style="color:var(--text-muted);">&mdash;</span>';

    const colLower = columnName.toLowerCase();

    // Grade Badges
    if (colLower === 'grade') {
        const gradeClass = `grade-${String(value).replace('+', '_plus')}`;
        return `<span class="grade-badge ${gradeClass}">${value}</span>`;
    }

    // Status Chips (Pass / Fail)
    if (colLower === 'status') {
        const isPass = String(value).toLowerCase() === 'pass';
        return `<span class="status-chip ${isPass ? 'pass' : 'fail'}">${value}</span>`;
    }

    // Attendance Progress Bars
    if (colLower.includes('attendance')) {
        const numVal = parseFloat(value);
        if (!isNaN(numVal)) {
            const isRisk = numVal < 75.0;
            return `
                <div class="attendance-cell">
                    <span>${numVal}%</span>
                    <div class="attendance-bar-bg">
                        <div class="attendance-bar-fill ${isRisk ? 'risk' : ''}" style="width: ${Math.min(numVal, 100)}%;"></div>
                    </div>
                </div>
            `;
        }
    }

    // Rank Badges
    if (colLower === 'rank') {
        const rankNum = parseInt(value, 10);
        if (rankNum === 1) return `<span class="rank-badge rank-gold">&#129351; 1</span>`;
        if (rankNum === 2) return `<span class="rank-badge rank-silver">&#129352; 2</span>`;
        if (rankNum === 3) return `<span class="rank-badge rank-bronze">&#129353; 3</span>`;
        return `#${rankNum}`;
    }

    return value;
}

function formatColumnTitle(columnName) {
    return columnName.replace(/_/g, ' ');
}

/**
 * ==============================================================================
 * DYNAMIC CHART.JS VISUALIZATION ENGINE
 * ==============================================================================
 */
function prepareAndRenderChart(queryType, data) {
    const chartCard = document.getElementById('chart-section');
    if (!chartCard || !data || data.length === 0) {
        hideChartSection();
        return;
    }

    let labels = [];
    let datasets = [];
    let chartTitle = QUERY_META[queryType]?.title || 'Analytics Chart';

    if (queryType === 'subject_avg') {
        labels = data.map(d => d.Subject);
        datasets = [{
            label: 'Average Marks',
            data: data.map(d => d.Average_Marks),
            backgroundColor: 'rgba(79, 70, 229, 0.75)',
            borderColor: '#4F46E5',
            borderWidth: 1.5,
            borderRadius: 6
        }];
        currentChartType = 'bar';
    } else if (queryType === 'dept_avg') {
        labels = data.map(d => d.Department);
        datasets = [{
            label: 'Department Average Marks',
            data: data.map(d => d.Average_Marks),
            backgroundColor: ['#4F46E5', '#0D9488', '#F59E0B', '#EF4444'],
            borderRadius: 8
        }];
        currentChartType = 'bar';
    } else if (queryType === 'grade_dist') {
        labels = data.map(d => d.Grade);
        datasets = [{
            label: 'Student Count',
            data: data.map(d => d.Student_Count),
            backgroundColor: [
                '#F59E0B', '#10B981', '#059669', '#3B82F6',
                '#6366F1', '#0D9488', '#94A3B8', '#EF4444'
            ],
            borderRadius: 6
        }];
        currentChartType = 'bar';
    } else if (queryType === 'pass_fail') {
        labels = data.map(d => d.Status);
        datasets = [{
            data: data.map(d => d.Count),
            backgroundColor: ['#10B981', '#EF4444'],
            borderWidth: 2,
            borderColor: document.documentElement.getAttribute('data-theme') === 'dark' ? '#131A29' : '#FFFFFF'
        }];
        currentChartType = 'doughnut';
    } else if (queryType === 'sem_perf') {
        labels = data.map(d => `Sem ${d.Semester}`);
        datasets = [
            {
                label: 'Average Marks',
                data: data.map(d => d.Average_Marks),
                borderColor: '#4F46E5',
                backgroundColor: 'rgba(79, 70, 229, 0.1)',
                tension: 0.35,
                fill: true
            },
            {
                label: 'Average Attendance (%)',
                data: data.map(d => d.Average_Attendance),
                borderColor: '#0D9488',
                backgroundColor: 'rgba(13, 148, 136, 0.1)',
                tension: 0.35,
                fill: true
            }
        ];
        currentChartType = 'line';
    } else if (queryType === 'top10') {
        labels = data.map(d => d.Student_Name || d.Student_ID);
        datasets = [{
            label: 'Total Marks',
            data: data.map(d => d.Marks),
            backgroundColor: 'rgba(99, 102, 241, 0.8)',
            borderColor: '#6366F1',
            borderRadius: 6
        }];
        currentChartType = 'bar';
    } else {
        hideChartSection();
        return;
    }

    currentChartData = { labels, datasets };
    document.getElementById('chart-title').textContent = chartTitle;
    document.getElementById('chart-subtitle').textContent = QUERY_META[queryType]?.sub || 'Visual Distribution';
    chartCard.style.display = 'block';

    updateChartTypeButtons(currentChartType);
    renderVisualChart(currentChartData);
}

function renderVisualChart(chartConfig) {
    const canvas = document.getElementById('analyticsChart');
    if (!canvas || !window.Chart) return;

    if (activeChart) {
        activeChart.destroy();
    }

    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const gridColor = isDark ? 'rgba(255, 255, 255, 0.06)' : 'rgba(0, 0, 0, 0.05)';
    const textColor = isDark ? '#94A3B8' : '#64748B';

    activeChart = new Chart(canvas, {
        type: currentChartType,
        data: chartConfig,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
                duration: 250,
                easing: 'easeOutQuart'
            },
            plugins: {
                legend: {
                    display: currentChartType === 'doughnut' || chartConfig.datasets.length > 1,
                    labels: { color: textColor, font: { family: 'Inter', size: 12 } }
                },
                tooltip: {
                    backgroundColor: isDark ? '#1E293B' : '#0F172A',
                    titleColor: '#FFFFFF',
                    bodyColor: '#F8FAFC',
                    padding: 10,
                    cornerRadius: 8
                }
            },
            scales: currentChartType === 'doughnut' ? {} : {
                x: {
                    grid: { color: gridColor },
                    ticks: { color: textColor, font: { family: 'Inter', size: 11 } }
                },
                y: {
                    beginAtZero: true,
                    grid: { color: gridColor },
                    ticks: { color: textColor, font: { family: 'Inter', size: 11 } }
                }
            }
        }
    });
}

function switchChartType(type) {
    currentChartType = type;
    updateChartTypeButtons(type);
    if (currentChartData) {
        renderVisualChart(currentChartData);
    }
}

function updateChartTypeButtons(activeType) {
    document.querySelectorAll('.chart-type-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-type') === activeType);
    });
}

function hideChartSection() {
    const chartCard = document.getElementById('chart-section');
    if (chartCard) chartCard.style.display = 'none';
    if (activeChart) {
        activeChart.destroy();
        activeChart = null;
    }
}

function downloadChartPng() {
    if (!activeChart) return;
    const imageUri = activeChart.toBase64Image();
    const link = document.createElement('a');
    link.download = `${currentActiveQuery || 'analytics'}_chart.png`;
    link.href = imageUri;
    link.click();
    showToast("Chart downloaded as PNG image.", "success");
}

/**
 * ==============================================================================
 * CLASS STATS PANEL RENDERER
 * ==============================================================================
 */
function renderClassStatsCards(statsList) {
    const statsContainer = document.getElementById('stats-display');
    if (!statsContainer) return;

    statsContainer.innerHTML = '';
    statsContainer.style.display = 'grid';

    statsList.forEach(item => {
        const card = document.createElement('div');
        card.className = 'metric-stat-card';
        card.innerHTML = `
            <div class="metric-stat-val">${item.Value}</div>
            <div class="metric-stat-lbl">${item.Metric}</div>
        `;
        statsContainer.appendChild(card);
    });
}

/**
 * ==============================================================================
 * DRAG & DROP UPLOAD CONTROLS
 * ==============================================================================
 */
function initDragAndDrop() {
    const dropzone = document.getElementById('upload-zone');
    if (!dropzone) return;

    ['dragenter', 'dragover'].forEach(eventName => {
        dropzone.addEventListener(eventName, (e) => {
            e.preventDefault();
            dropzone.classList.add('drag-active');
        });
    });

    ['dragleave', 'drop'].forEach(eventName => {
        dropzone.addEventListener(eventName, (e) => {
            e.preventDefault();
            dropzone.classList.remove('drag-active');
        });
    });

    dropzone.addEventListener('drop', (e) => {
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            uploadFile(files[0]);
        }
    });
}

function triggerFileUpload() {
    const fileInput = document.getElementById('file-input');
    if (fileInput) fileInput.click();
}

function handleFileSelected(event) {
    const files = event.target.files;
    if (files.length > 0) {
        uploadFile(files[0]);
    }
}

function uploadFile(file) {
    if (!file.name.endsWith('.csv')) {
        showToast("Please upload a valid CSV file (.csv)", "error");
        return;
    }

    const progressBar = document.getElementById('upload-progress');
    const progressFill = document.getElementById('progress-fill');
    if (progressBar) progressBar.style.display = 'block';
    if (progressFill) progressFill.style.width = '40%';

    showLoader("Ingesting and processing dataset...");

    // 1. Client-side instant parsing
    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            const parsed = parseCSV(e.target.result);
            if (parsed && parsed.length > 0) {
                activeDataset = parsed;
                // Clear client query cache so fresh calculations take effect
                Object.keys(CLIENT_QUERY_CACHE).forEach(k => delete CLIENT_QUERY_CACHE[k]);

                if (!isBackendOnline) {
                    if (progressFill) progressFill.style.width = '100%';
                    setTimeout(() => {
                        if (progressBar) progressBar.style.display = 'none';
                        if (progressFill) progressFill.style.width = '0%';
                    }, 500);

                    hideLoader();
                    updateStatus("offline", `Active: ${file.name} (${activeDataset.length.toLocaleString()} rows)`);
                    fetchPreview();
                    refreshKpiMetrics();
                    prefetchCommonQueries();
                    showToast(`Successfully loaded ${activeDataset.length.toLocaleString()} student records!`, "success");
                }
            }
        } catch (err) {
            console.warn("Client CSV parse note:", err);
        }
    };
    reader.readAsText(file);

    // 2. Synchronize with Spark Flask backend if online
    if (isBackendOnline) {
        const formData = new FormData();
        formData.append('file', file);

        fetch(`${BACKEND_URL}/upload`, {
            method: 'POST',
            body: formData
        })
            .then(res => res.json())
            .then(data => {
                if (progressFill) progressFill.style.width = '100%';
                setTimeout(() => {
                    if (progressBar) progressBar.style.display = 'none';
                    if (progressFill) progressFill.style.width = '0%';
                }, 600);

                hideLoader();
                // Clear cache on new backend dataset
                Object.keys(CLIENT_QUERY_CACHE).forEach(k => delete CLIENT_QUERY_CACHE[k]);

                if (data.status === "success") {
                    showToast(`Dataset "${data.filename}" successfully loaded into Spark!`, "success");
                    updateStatus("ready", `Active: ${data.filename} (${data.count.toLocaleString()} rows)`);
                    enableControls(true);
                    fetchPreview();
                    refreshKpiMetrics();
                    prefetchCommonQueries();
                } else {
                    showToast(`Upload status: ${data.error}`, "error");
                    fetchPreview();
                    refreshKpiMetrics();
                }
            })
            .catch(error => {
                hideLoader();
                if (progressBar) progressBar.style.display = 'none';
                showToast(`Local view active (${activeDataset.length} rows). Server note: ${error.message}`, "info");
                fetchPreview();
                refreshKpiMetrics();
            });
    }
}

/**
 * Robust CSV parser for client-side ingestion of any student dataset.
 */
function parseCSV(text) {
    const lines = text.split(/\r\n|\n/).filter(line => line.trim().length > 0);
    if (lines.length < 2) return [];

    const rawHeaders = parseCSVRow(lines[0]);
    const headers = rawHeaders.map(h => h.trim().replace(/^"|"$/g, ''));

    const findCol = (aliases) => {
        return headers.find(h => aliases.includes(h.toLowerCase().replace(/[\s_-]/g, '')));
    };

    const idCol = findCol(["studentid", "student_id", "rollno", "id", "roll_no"]) || headers[0];
    const nameCol = findCol(["studentname", "student_name", "name", "fullname", "full_name"]) || "Student_Name";
    const deptCol = findCol(["department", "dept", "branch", "stream"]) || "Department";
    const semCol = findCol(["semester", "sem", "term"]) || "Semester";
    const subCol = findCol(["subject", "sub", "course", "paper"]) || "Subject";
    const marksCol = findCol(["marks", "mark", "total", "score", "totalmarks", "total_marks"]) || "Marks";
    const attCol = findCol(["attendance", "att", "attendance%"]) || "Attendance";
    const gradeCol = findCol(["grade", "lettergrade", "class_grade"]) || "Grade";

    const records = [];
    for (let i = 1; i < lines.length; i++) {
        const row = parseCSVRow(lines[i]);
        if (row.length === 0) continue;
        const record = {};
        headers.forEach((h, idx) => { record[h] = row[idx] || ''; });

        const rawMarks = parseFloat(record[marksCol]);
        const marksVal = !isNaN(rawMarks) ? rawMarks : 0;
        const rawAtt = parseFloat(record[attCol]);
        const attVal = !isNaN(rawAtt) ? rawAtt : 0;

        let gradeVal = (record[gradeCol] || '').trim();
        if (!gradeVal) {
            gradeVal = marksVal >= 90 ? 'O' : marksVal >= 80 ? 'A+' : marksVal >= 70 ? 'A' : marksVal >= 60 ? 'B+' : marksVal >= 50 ? 'B' : marksVal >= 40 ? 'C' : marksVal >= 35 ? 'D' : 'F';
        }

        records.push({
            Student_ID: (record[idCol] || `S${10000 + i}`).trim(),
            Student_Name: (record[nameCol] || 'Unknown').trim(),
            Department: (record[deptCol] || 'General').trim(),
            Semester: String(record[semCol] || '1').trim(),
            Subject: (record[subCol] || 'General').trim(),
            Marks: marksVal,
            Attendance: attVal,
            Grade: gradeVal
        });
    }
    return records;
}

function parseCSVRow(rowStr) {
    const result = [];
    let cur = '';
    let insideQuotes = false;
    for (let i = 0; i < rowStr.length; i++) {
        const char = rowStr[i];
        if (char === '"') {
            insideQuotes = !insideQuotes;
        } else if (char === ',' && !insideQuotes) {
            result.push(cur.trim());
            cur = '';
        } else {
            cur += char;
        }
    }
    result.push(cur.trim());
    return result;
}

/**
 * ==============================================================================
 * STATUS & LOADER UTILITIES
 * ==============================================================================
 */
function updateHeaderInfo(queryKey) {
    const meta = QUERY_META[queryKey] || { title: 'Analysis', sub: 'Custom DataFrame Output' };
    const breadcrumb = document.getElementById('current-view-title');
    const tableTitle = document.getElementById('table-title');

    if (breadcrumb) breadcrumb.textContent = meta.title;
    if (tableTitle) tableTitle.textContent = meta.title;
}

function updateStatus(state, message) {
    const dot = document.getElementById('status-dot');
    const text = document.getElementById('status-text');

    if (!dot || !text) return;

    dot.className = "status-indicator";
    text.textContent = message;

    if (state === "ready") {
        dot.classList.add("ready");
    } else if (state === "error") {
        dot.classList.add("error");
    }
}

function enableControls(enabled) {
    document.querySelectorAll('.nav-item').forEach(button => {
        button.disabled = !enabled;
    });
}

function showLoader(message) {
    const loader = document.getElementById('query-loader');
    const tableContainer = document.getElementById('table-container');
    const loaderText = document.getElementById('loader-message');

    if (loader) loader.style.display = 'flex';
    if (tableContainer) tableContainer.style.display = 'none';
    if (loaderText) loaderText.textContent = message;
}

function hideLoader() {
    const loader = document.getElementById('query-loader');
    const tableContainer = document.getElementById('table-container');

    if (loader) loader.style.display = 'none';
    if (tableContainer) tableContainer.style.display = 'block';
}

function showErrorInTable(errorMessage) {
    const tableHeader = document.getElementById('table-header');
    const tableBody = document.getElementById('table-body');
    if (tableHeader) tableHeader.innerHTML = '<th>Error Information</th>';
    if (tableBody) {
        tableBody.innerHTML = `
            <tr>
                <td style="color: var(--rose); font-weight: 500; text-align: center; padding: 2rem;">
                    ${errorMessage}
                </td>
            </tr>
        `;
    }
}
