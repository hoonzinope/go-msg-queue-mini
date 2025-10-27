(function() {
    let queue_name = "";
    document.addEventListener("DOMContentLoaded", function() {
        queue_name = window.location.pathname.split("/").pop();
        detailInfo.init();
    });


    const detailInfo = {
        cursor: null,
        init : function() {
            this.populateQueueName();
            this.callStatusQueueInfo(this.drawQueueStatus);
            this.callPeekMessages(this.getFilterOptions(), this.drawMessageListTbody);
            this.bindEvents();
        },
        populateQueueName: function() {
            const queueNameElement = document.getElementById("queue-name");
            if (queueNameElement) {
                queueNameElement.textContent = queue_name;
            }
        },

        callStatusQueueInfo : function(callback) {
            fetch('/api/v1/'+queue_name+'/status', {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                },
            })
            .then(response => {
                if (!response.ok) {
                    throw new Error('Network response was not ok ' + response.statusText);
                }
                return response.json();
            })
            .then(data => {
                callback(data);
            })
            .catch(error => {
                console.error('There was a problem with the fetch operation:', error);
            });
        },

        drawQueueStatus : function(data) {
            const queueStatus = data.queue_status;
            const tbody = document.getElementById('queue-status-body');
            const newTbody = document.createElement('tbody');
            newTbody.id = 'queue-status-body';
            
            const row = document.createElement('tr');
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
            tbody.replaceWith(newTbody);
        },

        bindEvents: function() {
            document.getElementById("refresh-status").addEventListener("click", () => {
                this.callStatusQueueInfo(this.drawQueueStatus);
            });
            document.getElementById('refresh-messages').addEventListener('click', () => {
                document.getElementById('message-list-body').innerHTML = ''; // Clear existing messages
                this.cursor = null; // Reset cursor
                this.callPeekMessages(this.getFilterOptions(), this.drawMessageListTbody);
            });
            document.getElementById('limit-select').addEventListener('change', () => {
                document.getElementById('message-list-body').innerHTML = ''; // Clear existing messages
                this.cursor = null; // Reset cursor
                this.callPeekMessages(this.getFilterOptions(), this.drawMessageListTbody);
            });
            document.getElementById('order-select').addEventListener('change', () => {
                document.getElementById('message-list-body').innerHTML = ''; // Clear existing messages
                this.cursor = null; // Reset cursor
                this.callPeekMessages(this.getFilterOptions(), this.drawMessageListTbody);
            });
            document.getElementById('preview-toggle').addEventListener('change', () => {
                document.getElementById('message-list-body').innerHTML = ''; // Clear existing messages
                this.cursor = null; // Reset cursor
                this.callPeekMessages(this.getFilterOptions(), this.drawMessageListTbody);
            });

            document.getElementById('more').addEventListener('click', () => {
                let options = detailInfo.getFilterOptions();
                detailInfo.callPeekMessages(options, detailInfo.drawMessageListTbody);
            });
        },
        getFilterOptions : function() {
            let cursor = this.cursor;
            let limit = Number(document.getElementById('limit-select').value) || 10;
            let order = document.getElementById('order-select').value || 'asc';
            let preview = document.getElementById('preview-toggle').checked;
            
            let options = {
                limit: limit, 
                order: order, 
                preview: preview
            }
            if (cursor != null) {
                options.cursor = cursor;
            }

            return options;
        },
        callPeekMessages: function(options, callback) {
            const group_name = "default";
            this.prev_order = options.order;
            const data = {
                group : group_name,
                options : options
            }
            fetch('/api/v1/'+queue_name+'/peek', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data),
            })
            .then(response => {
                if (response != undefined && !response.ok) {
                    throw new Error('Network response was not ok ' + response.statusText);
                }
                return response.json();
            })
            .then(data => {
                callback(data);
            })
            .catch(error => {
                console.error('There was a problem with the fetch operation:', error);
            });
        },
        drawMessageListTbody: function(data) {
            const tbody = document.getElementById('message-list-body');

            messages = data.messages;
            console.log('Peeked Messages:', messages);
            if (data == null || messages.length === 0) {
                alert('No messages found.');
                return;
            }

            for (const message of messages) {
                const row = document.createElement('tr');

                const idCell = document.createElement('td');
                idCell.textContent = message.id;
                row.appendChild(idCell);
                detailInfo.cursor = message.id; // Update cursor to the last message ID
                console.log('Updated cursor to:', detailInfo.cursor);

                const receiptCell = document.createElement('td');
                receiptCell.textContent = message.receipt;
                row.appendChild(receiptCell);

                const payloadCell = document.createElement('td');
                payloadCell.textContent = message.payload;
                row.appendChild(payloadCell);

                const insertedAtCell = document.createElement('td');
                insertedAtCell.textContent = convertInsertedAt(message.inserted_at);
                row.appendChild(insertedAtCell);

                tbody.appendChild(row);
            }
        }
    }

    function convertInsertedAt(time) {
        return time.replaceWith('T', ' ');
    }
})();