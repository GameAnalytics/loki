-record(store, {name,               % Name of the loki instance
                backend,            % Backend used
                lock_table,         % Reference to the lock table
                config,             % Config specific to the backend
                options             % Additional options
               }).
