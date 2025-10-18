(function() {
    
    document.addEventListener('DOMContentLoaded', function() {
        renderQueueTable();

        document.getElementById('create-queue-button').addEventListener('click', openCreateQueueModal);
        document.getElementById('submit-create-queue').addEventListener('click', createQueue);
        document.getElementById('close-create-queue').addEventListener('click', closeCreateQueueModal);
        document.getElementById('refresh-status-button').addEventListener('click', refreshQueueStatus);

        intervalRefresh();
    });

    function intervalRefresh() {
        setInterval(refreshQueueStatus, 5000); // Refresh every 5 seconds
    }

    function renderQueueTable() {
        fetch('/api/v1/status/all')
            .then(response => response.json())
            .then(data => {
                drawTbody(data);
            })
            .catch(error => {
                alert('Error fetching queue status: ' + error);
            });
    }

    function openCreateQueueModal() {
        const modal = document.getElementById('create-queue-modal');
        modal.style.display = 'block';
    }

    function createQueue() {
        const queueName = document.getElementById('new-queue-name').value;
        fetch('/api/v1/'+queueName+'/create', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                "X-API-KEY": "secret_api_key"
            },
            //body: JSON.stringify({ name: queueName })
        })
        .then(response => response.json())
        .then(data => {
            console.log('Create Queue Response:', data);
            if(data.error) {
                alert('Error creating queue: ' + data.error);
                return;
            }
            alert('Queue created successfully! ' + queueName);
            // Close the modal and refresh the queue status
            closeCreateQueueModal();
            refreshQueueStatus();
        })
        .catch(error => {
            console.error('Error creating queue:', error);
            alert('Error creating queue: ' + error);
        });
    }

    function closeCreateQueueModal() {
        const modal = document.getElementById('create-queue-modal');
        modal.style.display = 'none';
    }

    function refreshQueueStatus() {
        fetch('/api/v1/status/all')
            .then(response => response.json())
            .then(data => {
                drawTbody(data);
            })
            .catch(error => {
                console.error('Error fetching status all:', error);
            });
    }

    function drawTbody(data) {
        const tbody = document.getElementById('queue-status-body');
        tbody.innerHTML = ''; // Clear existing rows

        for (const [queueName, queueStatus] of Object.entries(data.all_queue_map)) {
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
            dlqMessagesCell.classList.add('dlq-messages-cell');
            row.appendChild(dlqMessagesCell);

            tbody.appendChild(row);
        }   
    }
})();