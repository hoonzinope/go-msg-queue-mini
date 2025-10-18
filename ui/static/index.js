(function() {
    
    document.addEventListener('DOMContentLoaded', function() {
        callStatusAndRenderQueueTable();

        document.getElementById('create-queue-button').addEventListener('click', openCreateQueueModal);
        document.getElementById('submit-create-queue').addEventListener('click', createQueue);
        document.getElementById('close-create-queue').addEventListener('click', closeCreateQueueModal);
        document.getElementById('refresh-status-button').addEventListener('click', callStatusAndRenderQueueTable);
        document.getElementById('search-button').addEventListener('click', callStatusAndRenderQueueTable);

        intervalRefresh();
    });

    function intervalRefresh() {
        setTimeout(() => {
            _callStatusAllApi(drawTbody, intervalRefresh);
        }, 5000); // Refresh every 5 seconds
    }

    function callStatusAndRenderQueueTable() {
        _callStatusAllApi(drawTbody);
    }

    function openCreateQueueModal() {
        const modal = document.getElementById('create-queue-modal');
        modal.classList.remove('hidden');
    }

    function createQueue() {
        let queueName = document.getElementById('new-queue-name').value;
        let apiKey = document.getElementById('api-key-input').value;
        queueName = encodeURIComponent(queueName.trim());
        apiKey = encodeURIComponent(apiKey.trim());
        console.log('Creating queue:', queueName, 'with API Key:', apiKey);
        if (!queueName || queueName.length === 0 || queueName === '') {
            alert('Queue name cannot be empty.');
            return;
        }

        fetch('/api/v1/'+queueName+'/create', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-API-KEY': apiKey
            },
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok ' + response.statusText);
            }
            return response.json();
        })
        .then(data => {
            console.log('Create Queue Response:', data);
            if(data.error) {
                alert('Error creating queue: ' + data.error);
                return;
            }
            alert('Queue created successfully! ' + queueName);
            // Close the modal and refresh the queue status
            closeCreateQueueModal();
            callStatusAndRenderQueueTable();
        })
        .catch(error => {
            console.error('Error creating queue:', error);
            alert('Error creating queue: ' + error);
        });
    }

    function closeCreateQueueModal() {
        const modal = document.getElementById('create-queue-modal');
        modal.classList.add('hidden');
        document.getElementById('new-queue-name').value = '';
        document.getElementById('api-key-input').value = '';
    }

    function drawTbody(data) {
        let searchKeyword = document.getElementById('search-input').value.trim().toLowerCase();
        const tbody = document.getElementById('queue-status-body');
        const newTbody = document.createElement('tbody');
        newTbody.id = 'queue-status-body';

        for (const [queueName, queueStatus] of Object.entries(data.all_queue_map)) {
            if (searchKeyword && !queueName.toLowerCase().includes(searchKeyword)) {
                continue; // Skip queues that don't match the search keyword
            }
            const row = document.createElement('tr');

            const nameCell = document.createElement('td');
            nameCell.textContent = queueName;
            row.appendChild(nameCell);

            const totalMessagesCell = document.createElement('td');
            totalMessagesCell.textContent = queueStatus.total_messages;
            row.appendChild(totalMessagesCell);

            const ackedMessagesCell = document.createElement('td');
            ackedMessagesCell.textContent = queueStatus.acked_messages;
            row.appendChild(ackedMessagesCell);

            const inFlightMessagesCell = document.createElement('td');
            inFlightMessagesCell.textContent = queueStatus.inflight_messages;
            row.appendChild(inFlightMessagesCell);

            const dlqMessagesCell = document.createElement('td');
            dlqMessagesCell.textContent = queueStatus.dlq_messages;
            if (queueStatus.dlq_messages > 0) {
                dlqMessagesCell.classList.add('dlq-high');
            }
            row.appendChild(dlqMessagesCell);

            newTbody.appendChild(row);
        }   
        tbody.replaceWith(newTbody);
    }

    function _callStatusAllApi(func, finallyFunc) {
        fetch('/api/v1/status/all')
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok ' + response.statusText);
            }
            return response.json();
        })
        .then(data => {
            func(data);
        })
        .catch(error => {
            alert('Error fetching queue status: ' + error);
            return null;
        })
        .finally(() => {
            if (finallyFunc) {
                finallyFunc();
            }
        });
    }
})();