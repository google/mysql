disable_query_log;
require r/true.require;
select (plugin_status = 'ACTIVE') as `TRUE`
from information_schema.plugins
where plugin_name LIKE '%federated%' limit 1;
enable_query_log;
