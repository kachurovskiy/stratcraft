// StratCraft Client-side JavaScript

// Custom candlestick plugin for Chart.js
const candlestickPlugin = {
    id: 'candlestick',
    afterDatasetsDraw: function (chart) {
        const ctx = chart.ctx;

        // Find the candlestick dataset
        let candlestickDataset = null;

        for (let i = 0; i < chart.data.datasets.length; i++) {
            const dataset = chart.data.datasets[i];
            const isCandlestickDataset =
                (dataset.label && dataset.label.includes('Candlesticks')) ||
                (dataset._candlestickData && dataset._candlestickData.length);

            if (isCandlestickDataset) {
                candlestickDataset = dataset;
                break;
            }
        }

        if (!candlestickDataset) {
            return;
        }

        // Use the stored OHLC data instead of the dataset data
        const candlestickData = candlestickDataset._candlestickData || candlestickDataset.data;

        const xScale = chart.scales.x;
        const yScale = chart.scales.y;

        ctx.save();

        candlestickData.forEach((candle, index) => {
            if (!candle || typeof candle !== 'object') return;

            // Check if we have OHLC data or simplified data
            if (!candle.o || !candle.h || !candle.l || !candle.c) {
                return;
            }

            // Prefer category index to avoid NaNs on duplicate labels; fall back to value+index
            let x = xScale.getPixelForValue(index);
            if (isNaN(x) || !isFinite(x)) {
                x = xScale.getPixelForValue(candle.x, index);
            }
            const openY = yScale.getPixelForValue(candle.o);
            const highY = yScale.getPixelForValue(candle.h);
            const lowY = yScale.getPixelForValue(candle.l);
            const closeY = yScale.getPixelForValue(candle.c);

            // Skip if coordinates are invalid
            if (isNaN(x) || !isFinite(x) || isNaN(openY) || !isFinite(openY)) {
                return;
            }

            const isBullish = candle.c >= candle.o;
            const bodyTop = Math.min(openY, closeY);
            const bodyBottom = Math.max(openY, closeY);
            const bodyHeight = Math.abs(closeY - openY);

            // Draw wick (high-low line)
            ctx.strokeStyle = isBullish ? '#28a745' : '#dc3545';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(x, highY);
            ctx.lineTo(x, lowY);
            ctx.stroke();

            // Draw body
            ctx.fillStyle = isBullish ? 'rgba(40, 167, 69, 0.8)' : 'rgba(220, 53, 69, 0.8)';
            ctx.strokeStyle = isBullish ? '#28a745' : '#dc3545';
            ctx.lineWidth = 1;

            const bodyWidth = Math.max(4, xScale.width / candlestickData.length * 0.6);
            const bodyLeft = x - bodyWidth / 2;


            ctx.fillRect(bodyLeft, bodyTop, bodyWidth, bodyHeight);
            ctx.strokeRect(bodyLeft, bodyTop, bodyWidth, bodyHeight);
        });

        ctx.restore();
    }
};

const TRADE_MARKER_LINE_WIDTH = 2;

// Plugin to render trade start/end markers and exit price overlays
const tradeMarkersPlugin = {
    id: 'tradeMarkers',
    afterDatasetsDraw: function (chart) {
        if (!chart || !chart.options || !chart.options.plugins) {
            return;
        }

        const pluginConfig = chart.options.plugins.tradeMarkers;
        if (!pluginConfig) {
            return;
        }

        const area = chart.chartArea;
        const xScale = chart.scales && chart.scales.x;
        const yScale = chart.scales && chart.scales.y;

        if (!area || !xScale || !yScale) {
            return;
        }

        const ctx = chart.ctx;
        const labels = chart.data && chart.data.labels ? chart.data.labels : [];

        const getXForIndex = (index) => {
            if (typeof index !== 'number' || index < 0) {
                return null;
            }

            let x = Number.isFinite(index) && typeof xScale.getPixelForValue === 'function'
                ? xScale.getPixelForValue(index)
                : NaN;

            if (!Number.isFinite(x) && labels[index] !== undefined && typeof xScale.getPixelForValue === 'function') {
                x = xScale.getPixelForValue(labels[index], index);
            }

            if (!Number.isFinite(x) && typeof xScale.getPixelForTick === 'function') {
                x = xScale.getPixelForTick(index);
            }

            if (!Number.isFinite(x)) {
                const spread = Math.max(labels.length - 1, 1);
                const ratio = spread === 0 ? 0 : index / spread;
                x = area.left + (area.right - area.left) * ratio;
            }

            return Number.isFinite(x) ? x : null;
        };

        const drawText = (text, x, y, align, baseline, color) => {
            if (!text) {
                return;
            }
            ctx.save();
            ctx.font = '12px sans-serif';
            ctx.fillStyle = color || '#6c757d';
            ctx.textAlign = align;
            ctx.textBaseline = baseline;
            ctx.fillText(text, x, y);
            ctx.restore();
        };

        const drawVerticalLine = (marker) => {
            if (!marker) {
                return;
            }

            const x = getXForIndex(marker.index);
            if (!Number.isFinite(x)) {
                return;
            }

            const color = marker.color || '#0dcaf0';
            const dash = Array.isArray(marker.dash) ? marker.dash : [4, 4];

            ctx.save();
            ctx.strokeStyle = color;
            ctx.lineWidth = typeof marker.lineWidth === 'number' ? marker.lineWidth : TRADE_MARKER_LINE_WIDTH;
            ctx.setLineDash(dash);
            ctx.beginPath();
            ctx.moveTo(x, area.top);
            ctx.lineTo(x, area.bottom);
            ctx.stroke();
            ctx.restore();

            drawText(marker.text, x, area.top + 4, 'center', 'top', color);
        };

        const drawHorizontalLine = (marker) => {
            if (!marker || !Number.isFinite(marker.value)) {
                return;
            }

            const y = yScale.getPixelForValue(marker.value);
            if (!Number.isFinite(y)) {
                return;
            }

            const color = marker.color || '#20c997';
            const dash = Array.isArray(marker.dash) ? marker.dash : [6, 4];

            ctx.save();
            ctx.strokeStyle = color;
            ctx.lineWidth = typeof marker.lineWidth === 'number' ? marker.lineWidth : TRADE_MARKER_LINE_WIDTH;
            ctx.setLineDash(dash);
            ctx.beginPath();
            ctx.moveTo(area.left, y);
            ctx.lineTo(area.right, y);
            ctx.stroke();
            ctx.restore();

            drawText(marker.text, area.right - 6, y - 4, 'right', 'bottom', color);
        };

        drawVerticalLine(pluginConfig.start);
        drawVerticalLine(pluginConfig.end);
        drawHorizontalLine(pluginConfig.exitPrice);
    }
};

// Register custom Chart.js plugins globally
function registerChartPlugins() {
    if (typeof Chart !== 'undefined') {
        Chart.register(candlestickPlugin, tradeMarkersPlugin);
    } else {
        // Wait for Chart.js to load
        setTimeout(registerChartPlugins, 50);
    }
}

// Start registration process
registerChartPlugins();

document.addEventListener('DOMContentLoaded', function () {
    // Initialize tooltips and interactive elements
    initializeTooltips();
    initializeCharts();
    initializeForms();
    initializeClickableRows();
});

function initializeTooltips() {
    // Add tooltip functionality to elements with data-tooltip attribute
    const tooltipElements = document.querySelectorAll('[data-tooltip]');
    tooltipElements.forEach(element => {
        element.addEventListener('mouseenter', showTooltip);
        element.addEventListener('mouseleave', hideTooltip);
    });
}

function showTooltip(event) {
    const element = event.target;
    const tooltipText = element.getAttribute('data-tooltip');

    const tooltip = document.createElement('div');
    tooltip.className = 'tooltip';
    tooltip.textContent = tooltipText;
    tooltip.style.cssText = `
        position: absolute;
        background: #333;
        color: white;
        padding: 0.5rem;
        border-radius: 4px;
        font-size: 0.8rem;
        z-index: 1000;
        pointer-events: none;
    `;

    document.body.appendChild(tooltip);

    const rect = element.getBoundingClientRect();
    tooltip.style.left = rect.left + (rect.width / 2) - (tooltip.offsetWidth / 2) + 'px';
    tooltip.style.top = rect.top - tooltip.offsetHeight - 5 + 'px';

    element._tooltip = tooltip;
}

function hideTooltip(event) {
    const element = event.target;
    if (element._tooltip) {
        document.body.removeChild(element._tooltip);
        element._tooltip = null;
    }
}

function initializeClickableRows() {
    document.addEventListener('click', function (event) {
        const row = event.target.closest('.clickable-row');
        if (row) {
            const href = row.getAttribute('data-href');
            if (href) {
                event.preventDefault();
                event.stopPropagation();
                window.open(href, "_blank");
            }
        }
    });
}

function initializeCharts() {
    // Initialize any charts on the page
    const chartElements = document.querySelectorAll('[data-chart]');
    chartElements.forEach(element => {
        const chartType = element.getAttribute('data-chart');
        const chartData = JSON.parse(element.getAttribute('data-chart-data') || '{}');

        createChart(element, chartType, chartData);
    });
}

// Legacy chart functions - kept for backward compatibility with data-chart attributes
function createChart(canvas, type, data) {
    const ctx = canvas.getContext('2d');

    const defaultOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: true,
                position: 'top'
            }
        },
        scales: {
            x: {
                display: true,
                title: {
                    display: true,
                    text: 'Date'
                }
            },
            y: {
                display: true,
                title: {
                    display: true,
                    text: 'Price'
                }
            }
        }
    };

    switch (type) {
        case 'candlestick':
            createLegacyCandlestickChart(ctx, data, defaultOptions);
            break;
        case 'line':
            createLegacyLineChart(ctx, data, defaultOptions);
            break;
        case 'performance':
            createLegacyPerformanceChart(ctx, data, defaultOptions);
            break;
        default:
            console.warn('Unknown chart type:', type);
    }
}

function createLegacyCandlestickChart(ctx, data, options) {
    // Simple line chart for legacy data-chart attributes
    const chartData = {
        labels: data.labels || [],
        datasets: [{
            label: 'Price',
            data: data.prices || [],
            borderColor: '#667eea',
            backgroundColor: 'rgba(102, 126, 234, 0.1)',
            tension: 0.1
        }]
    };

    new Chart(ctx, {
        type: 'line',
        data: chartData,
        options: options
    });
}

function createLegacyLineChart(ctx, data, options) {
    const chartData = {
        labels: data.labels || [],
        datasets: data.datasets || []
    };

    new Chart(ctx, {
        type: 'line',
        data: chartData,
        options: options
    });
}

function createLegacyPerformanceChart(ctx, data, options) {
    const chartData = {
        labels: data.labels || [],
        datasets: [{
            label: 'Portfolio Value',
            data: data.values || [],
            borderColor: '#28a745',
            backgroundColor: 'rgba(40, 167, 69, 0.1)',
            tension: 0.1
        }]
    };

    new Chart(ctx, {
        type: 'line',
        data: chartData,
        options: {
            ...options,
            scales: {
                ...options.scales,
                y: {
                    ...options.scales.y,
                    title: {
                        display: true,
                        text: 'Portfolio Value ($)'
                    }
                }
            }
        }
    });
}

function initializeForms() {
    // Add form validation and enhancement
    const forms = document.querySelectorAll('form');
    forms.forEach(form => {
        form.addEventListener('submit', handleFormSubmit);
    });

    // Add real-time validation to input fields
    const inputs = document.querySelectorAll('input[required], select[required]');
    inputs.forEach(input => {
        input.addEventListener('blur', validateField);
        input.addEventListener('input', clearFieldError);
    });
}

function handleFormSubmit(event) {
    const form = event.target;
    const submitButton = form.querySelector('button[type="submit"]');
    if (submitButton && !event.defaultPrevented) {
        submitButton.disabled = true;
        submitButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';
    }
}
function validateField(event) {
    const field = event.target;
    const value = field.value.trim();

    clearFieldError(event);

    if (field.hasAttribute('required') && !value) {
        showFieldError(field, 'This field is required');
        return false;
    }

    if (field.type === 'email' && value && !isValidEmail(value)) {
        showFieldError(field, 'Please enter a valid email address');
        return false;
    }

    if (field.type === 'number') {
        const min = field.getAttribute('min');
        const max = field.getAttribute('max');

        if (min && parseFloat(value) < parseFloat(min)) {
            showFieldError(field, `Value must be at least ${min}`);
            return false;
        }

        if (max && parseFloat(value) > parseFloat(max)) {
            showFieldError(field, `Value must be at most ${max}`);
            return false;
        }
    }

    return true;
}

function showFieldError(field, message) {
    field.classList.add('error');

    let errorElement = field.parentNode.querySelector('.field-error');
    if (!errorElement) {
        errorElement = document.createElement('div');
        errorElement.className = 'field-error';
        errorElement.style.cssText = `
            color: #dc3545;
            font-size: 0.8rem;
            margin-top: 0.25rem;
        `;
        field.parentNode.appendChild(errorElement);
    }

    errorElement.textContent = message;
}

function clearFieldError(event) {
    const field = event.target;
    field.classList.remove('error');

    const errorElement = field.parentNode.querySelector('.field-error');
    if (errorElement) {
        errorElement.remove();
    }
}

function isValidEmail(email) {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
}

// Utility functions
function getIsoParts(value) {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        return null;
    }
    const iso = parsed.toISOString();
    return {
        date: iso.slice(0, 10),
        time: iso.slice(11, 19)
    };
}

function formatCurrency(amount) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD'
    }).format(amount);
}

function formatRateAsPercent(value) {
    // Handle both decimal (0.1 = 10%) and percentage (10 = 10%) formats
    const decimalValue = value > 1 ? value / 100 : value;
    return new Intl.NumberFormat('en-US', {
        style: 'percent',
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    }).format(decimalValue);
}

function formatDate(date) {
    const parts = getIsoParts(date);
    return parts ? parts.date : 'Invalid Date';
}

function formatDateTime(date) {
    const parts = getIsoParts(date);
    return parts ? `${parts.date} ${parts.time}` : 'Invalid Date';
}

function getCsrfToken() {
    const meta = document.querySelector('meta[name="csrf-token"]');
    return meta?.getAttribute('content') || '';
}

// API helper functions
async function apiRequest(url, options = {}) {
    const defaultOptions = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const config = { ...defaultOptions, ...options };
    const method = (config.method || 'GET').toUpperCase();
    if (!['GET', 'HEAD', 'OPTIONS'].includes(method)) {
        const csrfToken = getCsrfToken();
        if (csrfToken) {
            config.headers = {
                ...config.headers,
                'X-CSRF-Token': csrfToken
            };
        }
    }

    try {
        const response = await fetch(url, config);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error('API request failed:', error);
        throw error;
    }
}

window.csrfFetch = async function(url, options = {}) {
    const config = { ...options };
    const method = (config.method || 'GET').toUpperCase();

    if (!['GET', 'HEAD', 'OPTIONS'].includes(method)) {
        const csrfToken = getCsrfToken();
        if (csrfToken) {
            config.headers = {
                ...config.headers,
                'X-CSRF-Token': csrfToken
            };
        }
    }

    return fetch(url, config);
}

// Chart utility functions
const ChartUtils = {
    // Create candlestick data from raw candle data
    createCandlestickData: function (chartData, labelFormatter) {
        const formatLabel = typeof labelFormatter === 'function'
            ? labelFormatter
            : (date) => formatDate(date);

        return chartData.map(candle => ({
            x: formatLabel(candle.date),
            o: candle.open,
            h: candle.high,
            l: candle.low,
            c: candle.close
        }));
    },

    // Create candlestick dataset configuration
    createCandlestickDataset: function (label, candlestickData) {
        // Create a dataset with close prices for proper scaling, but hide the line
        const closePrices = candlestickData.filter(candle => candle !== null).map(candle => ({
            x: candle.x,
            y: candle.c
        }));

        return {
            label: label,
            data: closePrices,
            type: 'line',
            borderColor: 'transparent',
            backgroundColor: 'transparent',
            pointRadius: 0,
            pointHoverRadius: 0,
            showLine: false,
            hidden: false,
            // Store the original OHLC data for the plugin to use (filter out nulls)
            _candlestickData: candlestickData.filter(candle => candle !== null)
        };
    },

    // Create volume dataset configuration
    createVolumeDataset: function (chartData, color = '#6c757d') {
        return {
            label: 'Volume (USD)',
            data: chartData.map(candle => candle.volumeShares * candle.close),
            borderColor: color,
            backgroundColor: color.replace('rgb', 'rgba').replace(')', ', 0.1)'),
            borderWidth: 1,
            fill: false,
            tension: 0.1,
            yAxisID: 'y1',
            pointRadius: 1,
            pointHoverRadius: 2,
            pointStyle: 'circle'
        };
    },

    // Create entry price line dataset
    createEntryPriceDataset: function (labels, entryPrice) {
        return {
            label: 'Entry Price',
            data: Array(labels.length).fill(entryPrice),
            borderColor: '#0d6efd',
            borderWidth: TRADE_MARKER_LINE_WIDTH,
            backgroundColor: 'rgba(13, 110, 253, 0.1)',
            borderDash: [4, 4],
            fill: false,
            pointRadius: 0,
            pointHoverRadius: 0,
            showLine: true
        };
    },

    // Create stop loss line dataset
    createStopLossDataset: function (labels, stopLoss) {
        return {
            label: 'Stop Loss',
            data: Array(labels.length).fill(stopLoss),
            borderColor: '#dc3545',
            borderWidth: TRADE_MARKER_LINE_WIDTH,
            backgroundColor: 'rgba(220, 53, 69, 0.1)',
            borderDash: [5, 5],
            fill: false,
            pointRadius: 0,
            pointHoverRadius: 0
        };
    },

    // Create price target line dataset
    createPriceTargetDataset: function (labels, priceTarget) {
        return {
            label: 'Expected Price Target',
            data: Array(labels.length).fill(priceTarget),
            borderColor: '#28a745',
            borderWidth: TRADE_MARKER_LINE_WIDTH,
            backgroundColor: 'rgba(40, 167, 69, 0.1)',
            borderDash: [5, 5],
            fill: false,
            pointRadius: 0,
            pointHoverRadius: 0,
            showLine: true
        };
    },

    // Create simulated future price dataset
    createSimulatedPriceDataset: function (simulatedData) {
        return {
            label: 'Simulated Future Price',
            data: simulatedData,
            borderColor: '#ff6b35',
            backgroundColor: 'rgba(255, 107, 53, 0.1)',
            fill: false,
            tension: 0.1,
            pointRadius: 0,
            pointHoverRadius: 4,
            borderDash: [3, 3]
        };
    },

    // Create default chart options
    createDefaultOptions: function (includeVolume = false) {
        const options = {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: false
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: false
                    },
                    ticks: {
                        callback: function (value) {
                            return '$' + value.toFixed(2);
                        }
                    }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top'
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            const datasetLabel = context.dataset.label;
                            const yAxisId = context.dataset.yAxisID;

                            if (datasetLabel && datasetLabel.includes('Candlesticks')) {
                                const candle = context.dataset._candlestickData[context.dataIndex];
                                return [
                                    'Open: $' + candle.o.toFixed(2),
                                    'High: $' + candle.h.toFixed(2),
                                    'Low: $' + candle.l.toFixed(2),
                                    'Close: $' + candle.c.toFixed(2)
                                ];
                            } else if (datasetLabel === 'Simulated Future Price') {
                                return 'Simulated Future Price: $' + context.parsed.y.toFixed(2);
                            } else if (datasetLabel === 'Stop Loss') {
                                return 'Stop Loss: $' + context.parsed.y.toFixed(2);
                            } else if (datasetLabel === 'Entry Price') {
                                return 'Entry Price: $' + context.parsed.y.toFixed(2);
                            } else if (datasetLabel === 'Expected Price Target') {
                                return 'Expected Price Target: $' + context.parsed.y.toFixed(2);
                            } else if (datasetLabel && datasetLabel.includes('Volume')) {
                                return 'Volume: $' + context.parsed.y.toLocaleString();
                            } else if (yAxisId === 'y2') {
                                return datasetLabel + ': ' + Number(context.parsed.y).toFixed(2);
                            }
                            return datasetLabel + ': $' + context.parsed.y.toFixed(2);
                        }
                    }
                }
            }
        };

        if (includeVolume) {
            options.scales.y1 = {
                type: 'linear',
                display: true,
                position: 'right',
                beginAtZero: true,
                grid: {
                    drawOnChartArea: false,
                },
                ticks: {
                    callback: function (value) {
                        return '$' + value.toLocaleString();
                    }
                }
            };
        }

        return options;
    },

    // Deterministic color from a string id
    colorForId: function (id) {
        let hash = 0;
        for (let i = 0; i < id.length; i++) {
            hash = ((hash << 5) - hash) + id.charCodeAt(i);
            hash |= 0;
        }
        const hue = Math.abs(hash) % 360;
        const saturation = 70; // percent
        const lightness = 50; // percent
        return `hsl(${hue}, ${saturation}%, ${lightness}%)`;
    },

    // Create a complete candlestick chart
    createCandlestickChart: function (canvasId, chartData, options = {}) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) {
            console.error('Canvas element not found:', canvasId);
            return null;
        }

        const ctx = canvas.getContext('2d');
        const labelFormatter = typeof options.labelFormatter === 'function'
            ? options.labelFormatter
            : (date) => formatDate(date);

        const candlestickData = this.createCandlestickData(chartData, labelFormatter);
        const labels = candlestickData.map(candle => candle.x);

        const datasets = [
            this.createCandlestickDataset(options.candlestickLabel || 'Price (Candlesticks)', candlestickData)
        ];

        // Add volume if requested
        if (options.includeVolume) {
            datasets.push(this.createVolumeDataset(chartData, options.volumeColor));
        }

        const chartOptions = this.createDefaultOptions(options.includeVolume);

        // Merge custom options
        if (options.customOptions) {
            Object.assign(chartOptions, options.customOptions);
        }

        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: chartOptions
        });
    },

    // Create a trade detail chart with overlays
    createTradeDetailChart: function (canvasId, chartData, trade, options = {}) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) {
            console.error('Canvas element not found:', canvasId);
            return null;
        }

        const ctx = canvas.getContext('2d');
        const candlestickData = this.createCandlestickData(chartData);

        // Create labels for historical data
        let labels = chartData.map(candle => formatDate(candle.date));

        // Extend labels to include simulated future data dates if provided
        if (options.simulatedFutureData && options.simulatedFutureData.length > 0) {
            const futureLabels = options.simulatedFutureData.map(candle => formatDate(candle.date));
            labels = labels.concat(futureLabels);
        }

        // Create candlestick dataset with extended data to match labels
        const extendedCandlestickData = [...candlestickData];
        if (options.simulatedFutureData && options.simulatedFutureData.length > 0) {
            // Add null values for future periods where we don't have candlestick data
            for (let i = 0; i < options.simulatedFutureData.length; i++) {
                extendedCandlestickData.push(null);
            }
        }

        const datasets = [
            this.createCandlestickDataset('Price (Candlesticks)', extendedCandlestickData)
        ];

        // Add simulated future data if provided
        if (options.simulatedFutureData && options.simulatedFutureData.length > 0) {
            const simulatedData = [];
            // Fill with nulls for historical period
            for (let i = 0; i < chartData.length; i++) {
                simulatedData.push(null);
            }
            // Add connection point (same as last historical close price)
            simulatedData.push(chartData[chartData.length - 1].close);
            // Add simulated prices
            simulatedData.push(...options.simulatedFutureData.map(candle => candle.close));

            datasets.push(this.createSimulatedPriceDataset(simulatedData));
        }

        // Track trade entry index for marker overlays
        let tradeDateIndex = -1;
        const tradeLabel = formatDate(trade.date);
        if (tradeLabel && tradeLabel !== 'Invalid Date') {
            tradeDateIndex = labels.indexOf(tradeLabel);
        }

        // Add trade exit point for closed trades
        let exitDateIndex = -1;
        let exitDateLabel = null;
        if (trade.exitDate && trade.exitPrice) {
            const tradeExitLabel = formatDate(trade.exitDate);
            exitDateIndex = labels.indexOf(tradeExitLabel);

            if (exitDateIndex >= 0) {
                exitDateLabel = labels[exitDateIndex];
            }
        }

        // Attempt to locate expected end date so we can draw a placeholder marker for open trades
        let expectedEndIndex = -1;
        let expectedEndLabel = null;
        if (!trade.exitDate && options.expectedEndDate) {
            const expectedLabelCandidate = formatDate(options.expectedEndDate);
            if (expectedLabelCandidate && expectedLabelCandidate !== 'Invalid Date') {
                expectedEndIndex = labels.indexOf(expectedLabelCandidate);
                if (expectedEndIndex >= 0) {
                    expectedEndLabel = labels[expectedEndIndex];
                }
            }
        }

        // Add stop loss line if available
        if (trade.stopLoss) {
            datasets.push(this.createStopLossDataset(labels, trade.stopLoss));
        }

        // Add entry price line
        if (typeof trade.price === 'number' && isFinite(trade.price)) {
            datasets.push(this.createEntryPriceDataset(labels, trade.price));
        }

        // Add expected price target line if available
        if (options.expectedPriceTarget) {
            datasets.push(this.createPriceTargetDataset(labels, options.expectedPriceTarget));
        }

        const chartOptions = this.createDefaultOptions();

        // Configure marker overlays for trade lifecycle visualization
        const markerConfig = {};
        if (tradeDateIndex >= 0) {
            markerConfig.start = {
                index: tradeDateIndex,
                text: `Start: ${labels[tradeDateIndex]}`,
                color: '#0d6efd',
                dash: [4, 4],
                lineWidth: TRADE_MARKER_LINE_WIDTH
            };
        }

        if (exitDateIndex >= 0 && exitDateLabel) {
            markerConfig.end = {
                index: exitDateIndex,
                text: `End: ${exitDateLabel}`,
                color: '#fd7e14',
                dash: [4, 4],
                lineWidth: TRADE_MARKER_LINE_WIDTH
            };
        } else if (expectedEndIndex >= 0 && expectedEndLabel) {
            markerConfig.end = {
                index: expectedEndIndex,
                text: `Expected End: ${expectedEndLabel}`,
                color: '#ffc107',
                dash: [2, 2],
                lineWidth: TRADE_MARKER_LINE_WIDTH
            };
        }

        const exitPriceValue = trade.exitPrice !== undefined && trade.exitPrice !== null
            ? Number(trade.exitPrice)
            : NaN;

        if (Number.isFinite(exitPriceValue)) {
            markerConfig.exitPrice = {
                value: exitPriceValue,
                text: `Exit Price: ${formatCurrency(exitPriceValue)}`,
                color: '#20c997',
                dash: [6, 4],
                lineWidth: TRADE_MARKER_LINE_WIDTH
            };
        }

        if (Object.keys(markerConfig).length > 0) {
            chartOptions.plugins.tradeMarkers = markerConfig;
        }

        // Force the trade detail chart to use an exponential/logarithmic price scale
        chartOptions.scales.y.type = 'logarithmic';
        chartOptions.scales.y.title = {
            display: true,
            text: 'Price (Exponential Scale)'
        };

        const priceValues = [];
        const collectPrice = (value) => {
            if (typeof value === 'number' && isFinite(value) && value > 0) {
                priceValues.push(value);
            }
        };

        chartData.forEach(candle => {
            collectPrice(candle.open);
            collectPrice(candle.high);
            collectPrice(candle.low);
            collectPrice(candle.close);
        });

        if (options.simulatedFutureData && options.simulatedFutureData.length > 0) {
            options.simulatedFutureData.forEach(candle => collectPrice(candle.close));
        }

        collectPrice(trade.price);
        collectPrice(trade.stopLoss);
        collectPrice(trade.exitPrice);
        collectPrice(options.expectedPriceTarget);

        if (priceValues.length > 0) {
            const minPrice = Math.min(...priceValues);
            const maxPrice = Math.max(...priceValues);
            const lowerBound = Math.max(minPrice * 0.9, 0.01);
            const upperBound = maxPrice * 1.1;
            chartOptions.scales.y.min = lowerBound;
            chartOptions.scales.y.max = upperBound;
        }

        // Merge custom options
        if (options.customOptions) {
            Object.assign(chartOptions, options.customOptions);
        }

        return new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: chartOptions
        });
    }
};

function initializeTickerTradingViewChart(ticker, containerId) {
    if (!ticker) {
        return;
    }

    const resolvedContainerId = containerId || 'tradingViewChart';
    const container = document.getElementById(resolvedContainerId);
    if (!container) {
        return;
    }

    const theme = document.body.classList.contains('dark-theme') ? 'dark' : 'light';

    const loadWidget = () => {
        if (typeof TradingView === 'undefined' || typeof TradingView.widget !== 'function') {
            setTimeout(loadWidget, 100);
            return;
        }

        container.innerHTML = '';

        const widget = new TradingView.widget({
            autosize: true,
            symbol: ticker,
            interval: 'D',
            timezone: 'Etc/UTC',
            theme: theme,
            style: '1',
            locale: 'en',
            toolbar_bg: '#f1f3f6',
            hide_top_toolbar: false,
            hide_legend: false,
            withdateranges: false,
            allow_symbol_change: false,
            studies: [],
            container_id: resolvedContainerId,
            support_host: 'https://www.tradingview.com'
        });

    };

    loadWidget();
}

window.ChartUtils = ChartUtils;
window.initializeTickerTradingViewChart = initializeTickerTradingViewChart;

window.addEventListener('DOMContentLoaded', function () {
    const url = new URL(window.location);
    const hasAlert =
        url.searchParams.has('success') ||
        url.searchParams.has('error') ||
        url.searchParams.has('message');

    if (hasAlert) {
        // Remove alert parameters from URL
        url.searchParams.delete('success');
        url.searchParams.delete('error');
        url.searchParams.delete('message');

        // Update URL without page reload
        window.history.replaceState({}, '', url);
    }
});
