def _gen_ip_address(segments, port):
    rtn_val = []
    for seg in segments:
        rtn_val.append(str(seg))
    return ".".join(rtn_val), port


def _at_max(segments, max_val):
    for i in range(len(segments)):
        if segments[i] < max_val: return False

    return True


def _increment_address(segments, i_seg, max_val):
    if 0 <= i_seg < len(segments):
        segments[i_seg] += 1
        if segments[i_seg] > max_val:
            segments[i_seg] = 0
            _increment_address(segments, i_seg - 1, max_val)


def from_range(start_ip, end_ip, port):
    """
    Generates a list of node addresses using the given start and end ip addresses for the given
    port number
    :param start_ip:
    :param end_ip:
    :param port:
    :return:
    """
    node_list = []

    num_segments = 4
    seg_max = 255

    start_ip_segments = start_ip.split(".")
    end_ip_segments = end_ip.split(".")

    # check the given arguments are valid
    if len(start_ip_segments) < num_segments:
        raise ValueError("The given start_ip address must be in the form of xx.xx.xx.xx")
    if len(end_ip_segments) < num_segments:
        raise ValueError("The given end_ip address must be in the form of xx.xx.xx.xx")

    # parse the values given
    start_segments = []
    end_segments = []
    for seg in start_ip_segments:
        int_val = int(seg)
        if int_val > seg_max or int_val < 0:
            raise ValueError("Invalid value given for the start_ip address")
        else:
            start_segments.append(int_val)
    for seg in end_ip_segments:
        int_val = int(seg)
        if int_val > seg_max or int_val < 0:
            raise ValueError("Invalid value given for the end_ip address")
        else:
            end_segments.append(int_val)

    # now generate address values for the given range
    while start_segments != end_segments and not _at_max(start_segments, seg_max):
        node_list.append(_gen_ip_address(start_segments, port))

        # increment the ip address
        _increment_address(start_segments, len(start_segments) - 1, seg_max)

    # add on the final value
    node_list.append(_gen_ip_address(end_segments, port))

    return node_list
