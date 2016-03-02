-record(store, {name,               % Name of the loki instance
                backend,            % Backend used
                ref,                % Reference to the backend
                lock_table,         % Reference to the lock table
                config,             % Config specific to the backend
                options             % Additional options
               }).
