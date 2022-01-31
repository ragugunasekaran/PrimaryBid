import csv
import ast
import datetime


def transform_app_lifecycle():
    with open('CC_Application_Lifecycle.csv', newline='') as inbound_csv:
        reader = csv.reader(inbound_csv)
        next(reader)
        list_of_application_dtls = []
        for rows in reader:
            application_dict = {'UniqueID': 0, 'REGISTERED_0': None, 'ACKNOWLEDGED_0': None, 'APPROVED_0': None,
                                'REACKNOWLEDGED_0': None, 'CLOSED_0': None,
                                'APPOINTMENT_SCHEDULED_0': None, 'REJECTED_0': None, 'ON_HOLD_0': None,
                                'BLOCKED_0': None, 'TERMINATE_0': None, 'INITIATED_0': None,
                                'APPROVED_1': None, 'ON_HOLD_1': None, 'INITIATED_1': None, 'REGISTERED_1': None,
                                'BLOCKED_1': None, 'CLOSED_1': None, 'APPROVED_2': None}
            (id, acc_type, lifecycle_str) = rows
            reg_count = ack_count = app_count = reack_count = closed_count = schd_count = rej_count = on_hold_count = blk_count = ter_count = init_count = 0
            application_dict['UniqueID'] = id
            lifecycle_str_fmt = '[("' + lifecycle_str.replace('|', '"),("').replace(']', '","') + '")]'
            # print(lifecycle_str_fmt)
            event_records = list(ast.literal_eval(lifecycle_str_fmt))
            event_records_fmt = []
            for event in event_records:
                event_date_time = event[1].split('+')[0]
                event_date_time_fmt = datetime.datetime.strptime(event_date_time, '%Y-%m-%d %H:%M:%S.%f')
                event_date_time_str = event_date_time_fmt.strftime('%b %d %Y, %H:%M:%S')
                event_records_fmt.append((event[0], event_date_time_fmt, event_date_time_str))
            # print(type(event_records_fmt))
            event_records_sorted = sorted(event_records_fmt, key=lambda x: x[1])
            # print(type(event_records_sorted))
            for event in event_records_sorted:
                if event[0] == 'REGISTERED' and reg_count == 0:
                    application_dict['REGISTERED_0'] = event[2]
                    reg_count += 1
                elif event[0] == 'REGISTERED' and reg_count == 1:
                    application_dict['REGISTERED_1'] = event[2]
                    reg_count += 1
                elif event[0] == 'ACKNOWLEDGED' and ack_count == 0:
                    application_dict['ACKNOWLEDGED_0'] = event[2]
                    ack_count += 1
                elif event[0] == 'ACKNOWLEDGED' and ack_count == 1:
                    application_dict['ACKNOWLEDGED_1'] = event[2]
                    ack_count += 1
                elif event[0] == 'APPROVED' and app_count == 0:
                    application_dict['APPROVED_0'] = event[2]
                    app_count += 1
                elif event[0] == 'APPROVED' and app_count == 1:
                    application_dict['APPROVED_1'] = event[2]
                    app_count += 1
                elif event[0] == 'APPROVED' and app_count == 2:
                    application_dict['APPROVED_2'] = event[2]
                    app_count += 1
                elif event[0] == 'REACKNOWLEDGED' and reack_count == 0:
                    application_dict['REACKNOWLEDGED_0'] = event[2]
                    reack_count += 1
                elif event[0] == 'REACKNOWLEDGED' and reack_count == 1:
                    application_dict['REACKNOWLEDGED_1'] = event[2]
                    reack_count += 1
                elif event[0] == 'CLOSED' and closed_count == 0:
                    application_dict['CLOSED_0'] = event[2]
                    closed_count += 1
                elif event[0] == 'CLOSED' and closed_count == 1:
                    application_dict['CLOSED_1'] = event[2]
                    closed_count += 1
                elif event[0] == 'APPOINTMENT_SCHEDULED' and schd_count == 0:
                    application_dict['APPOINTMENT_SCHEDULED_0'] = event[2]
                    schd_count += 1
                elif event[0] == 'APPOINTMENT_SCHEDULED' and schd_count == 1:
                    application_dict['APPOINTMENT_SCHEDULED_1'] = event[2]
                    schd_count += 1
                elif event[0] == 'REJECTED' and rej_count == 0:
                    application_dict['REJECTED_0'] = event[2]
                    rej_count += 1
                elif event[0] == 'REJECTED' and rej_count == 1:
                    application_dict['REJECTED_1'] = event[2]
                    rej_count += 1
                elif event[0] == 'ON_HOLD' and on_hold_count == 0:
                    application_dict['ON_HOLD_0'] = event[2]
                    on_hold_count += 1
                elif event[0] == 'ON_HOLD' and on_hold_count == 1:
                    application_dict['ON_HOLD_1'] = event[2]
                    on_hold_count += 1
                elif event[0] == 'BLOCKED' and blk_count == 0:
                    application_dict['BLOCKED_0'] = event[2]
                    blk_count += 1
                elif event[0] == 'BLOCKED' and blk_count == 1:
                    application_dict['BLOCKED_1'] = event[2]
                    blk_count += 1
                elif event[0] == 'TERMINATE' and ter_count == 0:
                    application_dict['TERMINATE_0'] = event[2]
                    ter_count += 1
                elif event[0] == 'TERMINATE' and ter_count == 1:
                    application_dict['TERMINATE_1'] = event[2]
                    ter_count += 1
                elif event[0] == 'INITIATED' and init_count == 0:
                    application_dict['INITIATED_0'] = event[2]
                    init_count += 1
                elif event[0] == 'INITIATED' and init_count == 1:
                    application_dict['INITIATED_1'] = event[2]
                    init_count += 1
            list_of_application_dtls.append(application_dict)
        with open('Application_Lifecycle_Output.csv', 'w') as outbound_csv:
            keys = ['UniqueID', 'REGISTERED_0', 'ACKNOWLEDGED_0', 'APPROVED_0', 'REACKNOWLEDGED_0', 'CLOSED_0',
                    'APPOINTMENT_SCHEDULED_0', 'REJECTED_0', 'ON_HOLD_0', 'BLOCKED_0', 'TERMINATE_0', 'INITIATED_0',
                    'APPROVED_1', 'ON_HOLD_1', 'INITIATED_1', 'REGISTERED_1', 'BLOCKED_1', 'CLOSED_1', 'APPROVED_2']
            dict_writer = csv.DictWriter(outbound_csv, keys, lineterminator='\n')
            dict_writer.writeheader()
            dict_writer.writerows(list_of_application_dtls)


if __name__ == '__main__':
    transform_app_lifecycle()
