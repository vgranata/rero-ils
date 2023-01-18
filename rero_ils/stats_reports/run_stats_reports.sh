#!/bin/bash
#indicators=('number_of_checkouts' 'number_of_checkins' 'number_of_renewals' 'number_of_requests' 'number_of_documents' 'number_of_created_documents' 'number_of_items' 'number_of_created_items' 'number_of_deleted_items' 'number_of_holdings' 'number_of_created_holdings' 'number_of_patrons' 'number_of_ill_requests')
indicators=('number_of_checkouts' 'number_of_checkins' 'number_of_renewals' 'number_of_requests' 'number_of_documents' 'number_of_created_documents' 'number_of_items' 'number_of_created_items' 'number_of_deleted_items' 'number_of_holdings' 'number_of_created_holdings' 'number_of_ill_requests')

############
# statistics with no filters
############
# for indicator in ${indicators[@]}; do
#     echo "$indicator"
#     eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 1";
#     # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 2";
#     # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 3";
#     # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 1 -period year";
#     # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 2 -period year";
#     # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 3 -period year";
# done

############
# statistics with filter on date
############
for indicator in ${indicators[@]}; do
    echo "$indicator"
    if [[ "$indicator" == "number_of_checkouts" || "$indicator" == "number_of_checkins" || "$indicator" = "number_of_renewals" || "$indicator" = "number_of_requests" || "$indicator" = "number_of_created_documents" || "$indicator" = "number_of_created_items" || "$indicator" = "number_of_deleted_items" || "$indicator" = "number_of_created_holdings" ]]; then
        time_range='date'
        index='operation_logs'
    elif [[ "$indicator" == "number_of_documents" || "$indicator" == "number_of_items" || "$indicator" == "number_of_holdings" || "$indicator" == "number_of_ill_requests" ]]; then
        time_range='_created'
        if [[ "$indicator" == "number_of_documents" ]];then
            index='documents'
        elif [[ "$indicator" == "number_of_items" ]];then
            index='items'
        elif [[ "$indicator" == "number_of_holdings" ]];then
            index='holdings'
        elif [[ "$indicator" == "number_of_ill_requests" ]];then
            index='ill_requests'
        fi
    fi
    eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 1 -f1_index $index -f1 '$time_range:{2022-01-01T00:00:00 TO 2022-12-31T23:59:59}' -period year";
    # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 2 -f1_index $index -f1 '$time_range:{2022-01-01T00:00:00 TO 2022-12-31T23:59:59}' -period year";
    # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 3 -f1_index $index -f1 '$time_range:{2022-01-01T00:00:00 TO 2022-12-31T23:59:59}' -period year";
done