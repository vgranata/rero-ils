#!/bin/bash
indicators=('number_of_checkouts' 'number_of_checkins' 'number_of_renewals' 'number_of_requests' 'number_of_documents' 'number_of_created_documents' 'number_of_items' 'number_of_created_items' 'number_of_deleted_items' 'number_of_holdings' 'number_of_created_holdings' 'number_of_patrons' 'number_of_ill_requests')
for indicator in ${indicators[@]}; do
    echo "$indicator"
    eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 1";
    # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 2";
    # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 3";
    # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 1 -period year";
    # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 2 -period year";
    # eval "poetry run rero_ils/stats_reports/stats_report.py reroils stats report $indicator -org_pid 3 -period year";
done