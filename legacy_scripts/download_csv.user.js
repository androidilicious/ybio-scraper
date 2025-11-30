// ==UserScript==
// @name         Download Organizations Data as CSV (Bulk Pagination)
// @namespace    http://tampermonkey.net/
// @version      2.2
// @description  Extracts organization data from all pages and downloads as CSV chunks
// @author       You
// @match        https://ybio-brillonline-com.proxy.lib.duke.edu/ybio*
// @match        https://ybio.brillonline.com/ybio*
// @grant        none
// ==/UserScript==

(function () {
    'use strict';

    // ============ CONFIGURATION ============
    // Modify these values to distribute work among users
    const FROM_PAGE = 1;        // Start from this page (inclusive)
    const TO_PAGE = 3945;       // End at this page (inclusive)
    // Example distributions:
    // User 1: FROM_PAGE = 1,    TO_PAGE = 1000
    // User 2: FROM_PAGE = 1001, TO_PAGE = 2000
    // User 3: FROM_PAGE = 2001, TO_PAGE = 3000
    // User 4: FROM_PAGE = 3001, TO_PAGE = 3945
    // =======================================

    const CHUNK_SIZE = 2 * 1024 * 1024; // 2MB chunks
    const CSV_URL = '/ybio/csv?attach=page_3';
    const TOTAL_PAGES = TO_PAGE - FROM_PAGE + 1; // Calculate total pages in range
    const BASE_URL = '/ybio';

    // State management
    function getState() {
        return {
            isScraping: localStorage.getItem('ybio_isScraping') === 'true',
            csvBuffer: localStorage.getItem('ybio_csvBuffer') || '',
            chunkIndex: parseInt(localStorage.getItem('ybio_chunkIndex') || '1'),
            currentPage: parseInt(localStorage.getItem('ybio_currentPage') || FROM_PAGE.toString()),
            hasHeader: localStorage.getItem('ybio_hasHeader') === 'true'
        };
    }

    function setState(state) {
        localStorage.setItem('ybio_isScraping', state.isScraping);
        localStorage.setItem('ybio_csvBuffer', state.csvBuffer);
        localStorage.setItem('ybio_chunkIndex', state.chunkIndex);
        localStorage.setItem('ybio_currentPage', state.currentPage);
        localStorage.setItem('ybio_hasHeader', state.hasHeader);
    }

    function clearState() {
        localStorage.removeItem('ybio_isScraping');
        localStorage.removeItem('ybio_csvBuffer');
        localStorage.removeItem('ybio_chunkIndex');
        localStorage.removeItem('ybio_currentPage');
        localStorage.removeItem('ybio_hasHeader');
    }

    // Download function
    function downloadCSV(csv, filename) {
        const csvFile = new Blob([csv], { type: 'text/csv' });
        const downloadLink = document.createElement('a');
        downloadLink.download = filename;
        downloadLink.href = window.URL.createObjectURL(csvFile);
        downloadLink.style.display = 'none';
        document.body.appendChild(downloadLink);
        downloadLink.click();
        document.body.removeChild(downloadLink);
    }

    // Fetch CSV data for current page
    async function fetchCurrentPageCSV() {
        try {
            const response = await fetch(CSV_URL);
            if (!response.ok) throw new Error('Failed to fetch CSV');
            return await response.text();
        } catch (error) {
            console.error('Error fetching CSV:', error);
            return null;
        }
    }

    // Process CSV data (remove header if not first chunk)
    function processCSVData(csvText, includeHeader) {
        const lines = csvText.split('\n');
        if (!includeHeader && lines.length > 0) {
            // Remove header line
            lines.shift();
        }
        return lines.join('\n');
    }

    // Get URL for specific page number
    function getPageURL(pageNum) {
        if (pageNum === 1) {
            return BASE_URL;
        }
        return `${BASE_URL}?page=${pageNum - 1}`;
    }

    // Main scraping logic
    async function processScraping() {
        const state = getState();

        if (!state.isScraping) return;

        const pageInRange = state.currentPage - FROM_PAGE + 1;
        console.log(`Processing page ${state.currentPage} (${pageInRange}/${TOTAL_PAGES} in range ${FROM_PAGE}-${TO_PAGE})...`);

        // Fetch CSV data for current page
        const csvData = await fetchCurrentPageCSV();

        if (!csvData) {
            alert('Failed to fetch CSV data. Stopping scraping.');
            clearState();
            return;
        }

        // Process the CSV (include header only for first chunk)
        const processedData = processCSVData(csvData, !state.hasHeader);
        state.csvBuffer += processedData;
        state.hasHeader = true;

        // Check if buffer is too large, download chunk
        if (state.csvBuffer.length > CHUNK_SIZE) {
            const filename = `organizations_${FROM_PAGE}-${TO_PAGE}_part_${state.chunkIndex}.csv`;
            console.log(`Downloading chunk ${state.chunkIndex}...`);
            downloadCSV(state.csvBuffer, filename);
            state.csvBuffer = '';
            state.chunkIndex++;
        }

        // Move to next page
        state.currentPage++;
        setState(state);

        if (state.currentPage <= TO_PAGE) {
            console.log(`Navigating to page ${state.currentPage}...`);
            setTimeout(() => {
                window.location.href = getPageURL(state.currentPage);
            }, 1000); // Small delay to avoid rate limiting
        } else {
            // Last page - download remaining buffer
            console.log('All pages in range processed. Finalizing...');
            if (state.csvBuffer.trim()) {
                const filename = state.chunkIndex > 1
                    ? `organizations_${FROM_PAGE}-${TO_PAGE}_part_${state.chunkIndex}.csv`
                    : `organizations_${FROM_PAGE}-${TO_PAGE}_complete.csv`;
                downloadCSV(state.csvBuffer, filename);
            }
            clearState();
            alert(`Scraping complete! Downloaded ${state.chunkIndex} file(s) from pages ${FROM_PAGE}-${TO_PAGE}.`);
        }
    }

    // Start scraping
    function startScraping() {
        if (confirm(`This will scrape pages ${FROM_PAGE} to ${TO_PAGE} (${TOTAL_PAGES} pages) and download data in chunks. Continue?`)) {
            setState({
                isScraping: true,
                csvBuffer: '',
                chunkIndex: 1,
                currentPage: FROM_PAGE,
                hasHeader: false
            });
            // Navigate to first page in range
            window.location.href = getPageURL(FROM_PAGE);
        }
    }

    // Stop scraping
    function stopScraping() {
        const state = getState();
        const downloaded = state.chunkIndex > 1 ? `${state.chunkIndex - 1} chunk(s)` : 'no chunks';
        if (confirm(`Stop scraping at page ${state.currentPage}? You've downloaded ${downloaded} so far.`)) {
            // Download whatever is in the buffer
            if (state.csvBuffer.trim()) {
                downloadCSV(state.csvBuffer, `organizations_${FROM_PAGE}-${TO_PAGE}_part_${state.chunkIndex}_incomplete.csv`);
            }
            clearState();
            alert('Scraping stopped.');
            location.reload();
        }
    }

    // Download current page only
    async function downloadCurrentPage() {
        const csvData = await fetchCurrentPageCSV();
        if (csvData) {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            downloadCSV(csvData, `organizations_current_page_${timestamp}.csv`);
        } else {
            alert('Failed to download current page.');
        }
    }

    // Add UI buttons
    function addButtons() {
        const targetElement = document.getElementById('page-title') || document.querySelector('table.views-table');

        if (!targetElement) {
            console.warn('Target element for buttons not found.');
            return;
        }

        const container = document.createElement('div');
        container.style.marginTop = '10px';
        container.style.marginBottom = '10px';
        container.style.padding = '15px';
        container.style.backgroundColor = '#f5f5f5';
        container.style.borderRadius = '5px';
        container.style.border = '1px solid #ddd';

        // Show page range config
        const rangeInfo = document.createElement('div');
        rangeInfo.innerText = `📊 Page Range: ${FROM_PAGE}-${TO_PAGE} (${TOTAL_PAGES} pages)`;
        rangeInfo.style.fontWeight = 'bold';
        rangeInfo.style.marginBottom = '10px';
        rangeInfo.style.color = '#333';
        container.appendChild(rangeInfo);

        const state = getState();

        // Start Scraping button
        if (!state.isScraping) {
            const startBtn = document.createElement('button');
            startBtn.innerText = `Start Scraping Pages ${FROM_PAGE}-${TO_PAGE}`;
            startBtn.style.padding = '10px 20px';
            startBtn.style.backgroundColor = '#4CAF50';
            startBtn.style.color = 'white';
            startBtn.style.border = 'none';
            startBtn.style.cursor = 'pointer';
            startBtn.style.fontSize = '14px';
            startBtn.style.borderRadius = '5px';
            startBtn.style.marginRight = '10px';
            startBtn.onclick = startScraping;
            container.appendChild(startBtn);
        } else {
            // Stop button (shown when scraping)
            const stopBtn = document.createElement('button');
            stopBtn.innerText = 'Stop Scraping';
            stopBtn.style.padding = '10px 20px';
            stopBtn.style.backgroundColor = '#f44336';
            stopBtn.style.color = 'white';
            stopBtn.style.border = 'none';
            stopBtn.style.cursor = 'pointer';
            stopBtn.style.fontSize = '14px';
            stopBtn.style.borderRadius = '5px';
            stopBtn.style.marginRight = '10px';
            stopBtn.onclick = stopScraping;
            container.appendChild(stopBtn);

            // Status indicator
            const status = document.createElement('span');
            const pageInRange = state.currentPage - FROM_PAGE + 1;
            const progress = ((pageInRange / TOTAL_PAGES) * 100).toFixed(1);
            status.innerText = `⏳ Page ${state.currentPage} (${pageInRange}/${TOTAL_PAGES} = ${progress}%) | Chunk ${state.chunkIndex}`;
            status.style.color = '#ff9800';
            status.style.fontWeight = 'bold';
            status.style.marginLeft = '10px';
            container.appendChild(status);
        }

        // Download current page button
        const downloadBtn = document.createElement('button');
        downloadBtn.innerText = 'Download Current Page Only';
        downloadBtn.style.padding = '10px 20px';
        downloadBtn.style.backgroundColor = '#2196F3';
        downloadBtn.style.color = 'white';
        downloadBtn.style.border = 'none';
        downloadBtn.style.cursor = 'pointer';
        downloadBtn.style.fontSize = '14px';
        downloadBtn.style.borderRadius = '5px';
        downloadBtn.onclick = downloadCurrentPage;
        container.appendChild(downloadBtn);

        targetElement.parentNode.insertBefore(container, targetElement.nextSibling);
    }

    // Initialize
    window.addEventListener('load', () => {
        setTimeout(() => {
            addButtons();

            // If scraping is active, continue processing
            const state = getState();
            if (state.isScraping) {
                setTimeout(processScraping, 1500);
            }
        }, 1000);
    });

})();
