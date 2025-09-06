document.addEventListener('DOMContentLoaded', () => {
    // --- CONFIGURATION ---
    // IMPORTANT: Replace this with your actual API Gateway Invoke URL
    const API_BASE_URL = 'XXXXXXXXXXXXXXXXXXXXXX';

    // --- DOM ELEMENT SELECTORS ---
    const pages = document.querySelectorAll('.page');
    const navLinks = document.querySelectorAll('.nav-link');
    const parkingDataBody = document.getElementById('parking-data-body');
    const alertsList = document.getElementById('alerts-list');
    const modal = document.getElementById('manual-entry-modal');
    const closeModalBtn = document.querySelector('.close-button');

    // --- INITIALIZATION ---
    function initializeDashboard() {
        fetchDashboardStatus();
        fetchAlerts();
        setupEventListeners();
        // Set today's date in the log viewer
        document.getElementById('log-date').valueAsDate = new Date();
    }

    // --- NAVIGATION ---
    function setupEventListeners() {
        navLinks.forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const targetId = link.getAttribute('href').substring(1);

                pages.forEach(page => page.classList.remove('active'));
                document.getElementById(targetId + '-page').classList.add('active');

                navLinks.forEach(nav => nav.classList.remove('active'));
                link.classList.add('active');
            });
        });
        
        // Modal listeners
        closeModalBtn.addEventListener('click', () => modal.style.display = 'none');
        window.addEventListener('click', (e) => {
            if (e.target == modal) modal.style.display = 'none';
        });

        // Form submissions
        document.getElementById('add-slots-form').addEventListener('submit', handleAddSlots);
        document.getElementById('initialize-system-form').addEventListener('submit', handleInitializeSystem);
        document.getElementById('view-logs-form').addEventListener('submit', handleViewLogs);
        document.getElementById('manual-entry-form').addEventListener('submit', handleManualEntrySubmit);

        // Button clicks
        document.getElementById('reset-system-btn').addEventListener('click', handleResetSystem);
        document.getElementById('btn-set-maintenance').addEventListener('click', () => handleBatchChangeStatus('maintenance'));
        document.getElementById('btn-set-empty').addEventListener('click', () => handleBatchChangeStatus('empty'));
        document.getElementById('btn-delete-selected').addEventListener('click', handleBatchDelete);
        
        // Table-related listeners
        document.getElementById('select-all-slots').addEventListener('change', handleSelectAll);
        parkingDataBody.addEventListener('click', handleTableActions);
    }

    // --- API HELPER FUNCTION ---
    async function apiRequest(endpoint, method = 'GET', body = null) {
        const url = `${API_BASE_URL}${endpoint}`;
        const options = {
            method,
            headers: { 'Content-Type': 'application/json' },
        };
        if (body) {
            options.body = JSON.stringify(body);
        }

        try {
            const response = await fetch(url, options);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.error || `Request failed with status ${response.status}`);
            }
            // Handle responses that might not have a body
            const contentType = response.headers.get("content-type");
            if (contentType && contentType.indexOf("application/json") !== -1) {
                return await response.json();
            }
            return { success: true };

        } catch (error) {
            console.error(`API Error on ${method} ${endpoint}:`, error);
            showToast(error.message, 'error');
            throw error; // Re-throw to allow caller to handle
        }
    }


    // --- DATA FETCHING & RENDERING ---

    // Fetches main dashboard stats and slot list (from get_parking_status_lambda)
    async function fetchDashboardStatus() {
        try {
            const data = await apiRequest('/admin/status', 'GET');
            document.getElementById('total-spots').textContent = data.total_spots;
            document.getElementById('occupied-spots').textContent = data.occupied_spots;
            document.getElementById('available-spots').textContent = data.available_spots;
            document.getElementById('occupancy-rate').textContent = `${data.occupancy_rate.toFixed(1)}%`;
            renderParkingTable(data.slots);
        } catch (error) {
            console.error('Failed to fetch dashboard status.');
        }
    }

    // Renders the main table with all parking spots
    function renderParkingTable(slots) {
        parkingDataBody.innerHTML = ''; // Clear existing data
        if (!slots || slots.length === 0) {
            parkingDataBody.innerHTML = `<tr><td colspan="6">No parking spots found. Initialize the system.</td></tr>`;
            return;
        }

        slots.sort((a, b) => a.parking_id.localeCompare(b.parking_id)).forEach(slot => {
            const statusClass = `status-${slot.status.toLowerCase()}`;
            const isOccupied = slot.status === 'occupied';
            const isEmpty = slot.status === 'empty' || slot.status === 'available';

            const row = document.createElement('tr');
            row.innerHTML = `
                <td><input type="checkbox" class="slot-checkbox" data-id="${slot.parking_id}"></td>
                <td><b>${slot.parking_id}</b></td>
                <td><span class="status-badge ${statusClass}">${slot.status}</span></td>
                <td>${slot.vehicle_id || 'N/A'}</td>
                <td>${slot.email || 'N/A'}</td>
                <td>
                    <button class="btn-action exit" title="Manual Exit" data-action="exit" data-id="${slot.parking_id}" ${!isOccupied ? 'disabled' : ''}><i class="fas fa-sign-out-alt"></i></button>
                    <button class="btn-action entry" title="Manual Entry" data-action="entry" data-id="${slot.parking_id}" ${!isEmpty ? 'disabled' : ''}><i class="fas fa-sign-in-alt"></i></button>
                </td>
            `;
            parkingDataBody.appendChild(row);
        });
    }

    // Fetches overdue alerts (from view_alerts_lambda)
    async function fetchAlerts() {
        try {
            const alerts = await apiRequest('/admin/alerts', 'GET');
            alertsList.innerHTML = '';
            if (alerts.length === 0) {
                alertsList.innerHTML = '<li>No overdue vehicles.</li>';
                return;
            }
            alerts.forEach(alert => {
                const li = document.createElement('li');
                li.innerHTML = `<b>${alert.parking_id}</b> (${alert.vehicle_id}) is ${alert.overdue}.`;
                alertsList.appendChild(li);
            });
        } catch (error) {
            console.error('Failed to fetch alerts.');
        }
    }
    
    // --- EVENT HANDLERS for ACTIONS ---

    // Handles Manual Entry/Exit buttons in the table
    function handleTableActions(e) {
        const button = e.target.closest('.btn-action');
        if (!button) return;
        
        const action = button.dataset.action;
        const id = button.dataset.id;
        
        if (action === 'exit') {
            if (confirm(`Are you sure you want to manually vacate spot ${id}?`)) {
                handleManualExit(id);
            }
        } else if (action === 'entry') {
            openManualEntryModal(id);
        }
    }
    
    // Manual Exit (from manual_exit_lambda)
    async function handleManualExit(parkingId) {
        try {
            await apiRequest('/admin/slots/manual-override/manual-exit', 'POST', { parking_id: parkingId });
            showToast(`Slot ${parkingId} vacated successfully.`, 'success');
            fetchDashboardStatus(); // Refresh data
        } catch (error) {
             // Error toast is shown by apiRequest
        }
    }

    // Open Manual Entry Modal
    function openManualEntryModal(parkingId) {
        document.getElementById('manual-entry-form').reset();
        document.getElementById('modal-parking-id').value = parkingId;
        document.getElementById('modal-parking-id-display').textContent = parkingId;
        modal.style.display = 'flex';
    }

    // Manual Entry Form Submission (from manual_entry_lambda)
    async function handleManualEntrySubmit(e) {
        e.preventDefault();
        const parkingId = document.getElementById('modal-parking-id').value;
        const vehicleId = document.getElementById('modal-vehicle-id').value;
        const email = document.getElementById('modal-email').value;
        const expectedTime = parseInt(document.getElementById('modal-expected-time').value, 10);

        const body = {
            parking_id: parkingId,
            vehicle_id: vehicleId,
            email: email,
            expected_time_minutes: expectedTime
        };

        try {
            await apiRequest('/admin/slots/manual-override/manual-entry', 'POST', body);
            showToast(`Vehicle ${vehicleId} assigned to ${parkingId}.`, 'success');
            modal.style.display = 'none';
            fetchDashboardStatus(); // Refresh data
            fetchAlerts();
        } catch (error) {
            // Error toast is shown by apiRequest
        }
    }

    // Add Slots (from add_slots_lambda)
    async function handleAddSlots(e) {
        e.preventDefault();
        const area = parseInt(document.getElementById('area-number').value, 10);
        const floor = parseInt(document.getElementById('floor-number').value, 10);
        const count = parseInt(document.getElementById('new-slots-count').value, 10);

        try {
            const data = await apiRequest('/admin/slots/add', 'POST', {
                area_number: area,
                floor_number: floor,
                new_slots: count
            });
            showToast(data.message, 'success');
            e.target.reset();
            fetchDashboardStatus(); // Refresh dashboard
        } catch (error) {
            // Error handled by apiRequest
        }
    }

    // Initialize System (from initialize_system_lambda)
    async function handleInitializeSystem(e) {
        e.preventDefault();
        const layoutInput = document.getElementById('parking-layout').value;
        try {
            const layout = JSON.parse(layoutInput);
            if(confirm("Are you sure you want to initialize the system? This will add new slots but won't remove existing ones.")) {
                const data = await apiRequest('/admin/system/initialize', 'POST', { parking_layout: layout });
                showToast(data.message, 'success');
                fetchDashboardStatus(); // Refresh dashboard
            }
        } catch (jsonError) {
            showToast('Invalid JSON format for parking layout.', 'error');
        }
    }

    // Reset System (from reset_system_lambda)
    async function handleResetSystem() {
        if(prompt("This will delete ALL parking spots. This action is irreversible. To confirm, type 'RESET' below:") === 'RESET') {
            try {
                const data = await apiRequest('/admin/system/reset', 'POST', {});
                showToast(data.message, 'success');
                fetchDashboardStatus(); // Refresh dashboard
                fetchAlerts();
            } catch (error) {
                // Error handled by apiRequest
            }
        } else {
            showToast('Reset action cancelled.', 'secondary');
        }
    }

    // View Vehicle Logs (from view_vehicle_logs_lambda)
    async function handleViewLogs(e) {
        e.preventDefault();
        const date = document.getElementById('log-date').value;
        const logsBody = document.getElementById('vehicle-logs-body');
        
        try {
            const logs = await apiRequest('/admin/system/view-logs', 'POST', { date });
            document.getElementById('displayed-log-date').textContent = date;
            logsBody.innerHTML = '';
            if(logs.length === 0) {
                logsBody.innerHTML = `<tr><td colspan="4">No vehicle logs found for this date.</td></tr>`;
                return;
            }
            logs.forEach(log => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${log.vehicleId}</td>
                    <td>${log.parkingId}</td>
                    <td>${new Date(log.entryTime).toLocaleString()}</td>
                    <td>${new Date(log.exitTime).toLocaleString()}</td>
                `;
                logsBody.appendChild(row);
            });
            // logs.forEach(log => {
            //     const row = document.createElement('tr');
            //     row.innerHTML = `
            //         <td>${log.vehicle_id}</td>
            //         <td>${log.parking_id}</td>
            //         <td>${new Date(log.entry_time).toLocaleString()}</td>
            //         <td>${new Date(log.exit_time).toLocaleString()}</td>
            //     `;
            //     logsBody.appendChild(row);
            // });

        } catch (error) {
            logsBody.innerHTML = `<tr><td colspan="4">Failed to load logs.</td></tr>`;
        }
    }

    // --- BATCH ACTION HANDLERS ---
    function getSelectedSlotIds() {
        return Array.from(document.querySelectorAll('.slot-checkbox:checked')).map(cb => cb.dataset.id);
    }
    
    function handleSelectAll(e) {
        const checkboxes = document.querySelectorAll('.slot-checkbox');
        checkboxes.forEach(checkbox => checkbox.checked = e.target.checked);
    }

    // Batch Change Status (from change_slot_status_lambda)
    async function handleBatchChangeStatus(newStatus) {
        const selectedIds = getSelectedSlotIds();
        if (selectedIds.length === 0) {
            showToast('No slots selected.', 'error');
            return;
        }
        if (confirm(`Are you sure you want to change the status of ${selectedIds.length} slots to '${newStatus}'?`)) {
            try {
                const data = await apiRequest('/admin/slots/change-status', 'POST', {
                    parking_ids: selectedIds,
                    status: newStatus
                });
                showToast(data.message, 'success');
                fetchDashboardStatus(); // Refresh
            } catch (error) {
                // Error handled by apiRequest
            }
        }
    }
    
    // Batch Delete (from delete_slot_lambda)
    async function handleBatchDelete() {
        const selectedIds = getSelectedSlotIds();
        if (selectedIds.length === 0) {
            showToast('No slots selected.', 'error');
            return;
        }
        if (confirm(`Are you sure you want to permanently delete ${selectedIds.length} slots? This cannot be undone.`)) {
             try {
                const data = await apiRequest('/admin/slots/batch-remove', 'POST', {
                    parking_ids: selectedIds
                });
                showToast(data.message, 'success');
                fetchDashboardStatus(); // Refresh
            } catch (error) {
                // Error handled by apiRequest
            }
        }
    }

    // --- UI HELPER: TOAST NOTIFICATION ---
    function showToast(message, type = 'success') {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.textContent = message;

        container.appendChild(toast);
        setTimeout(() => toast.classList.add('show'), 10); // Animate in
        setTimeout(() => {
            toast.classList.remove('show');
            toast.addEventListener('transitionend', () => toast.remove());
        }, 5000); // Animate out and remove
    }

    // --- RUN APPLICATION ---
    initializeDashboard();

});
