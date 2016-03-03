-record(store, {name,               % Name of the loki instance
                mod,                % Backend used
                backend,            % Backend config
                lock_table,         % Reference to the lock table
                options             % Additional options
               }).

-record(backend, {ref,              % Reference to the backend
                  options}).        % Options specific to the backend
