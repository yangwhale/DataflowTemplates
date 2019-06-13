function trans(line, param) {
    if (!param)
        param = "NULL";

    var values = line.split('\t');

    var obj = new Object();
    obj.timestamp_field_0 = values[0].split('\x01')[1];
    obj.int64_field_1 = values[1].split('\x01')[1];
    obj.string_field_2 = values[2].split('\x01')[1];
    obj.string_field_3 = values[3].split('\x01')[1] + " - " + param;
    obj.int64_field_4 = values[4].split('\x01')[1];
    obj.string_field_5 = values[5].split('\x01')[1];
    obj.string_field_6 = values[6].split('\x01')[1];

    var jsonStr = JSON.stringify(obj);

    return jsonStr;
}
