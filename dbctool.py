"""A tool to read and manipulate dbc files.

The module  has a Parser class with a metohd  that parses the content of a \
dbc file held in a string and returns the content on an intermediate format \
in the form of a list of parsed dbc-sections. This intermediate format can \
be used to initialize an object of the Bus class. The objects of the Bus \
class can hold the same information as a dbc-file can, but does so in a more \
structured form. Most, but not all, dbc sections are implemented. The class \
also has a method to generate a dbc representation of the object's content.
"""
#
# Copyright 2024 Einar Halvorsen
#
# License: GPL-3.0-or-later
#
import sys
import re
import warnings
import textwrap
import pprint


def custom_formatwarning(msg, *args, **kwargs):
    return str(args[0].__name__) + ": " + str(msg) + '\n'


warnings.formatwarning = custom_formatwarning


class DatabaseError(Exception):
    """Semantic errors in database.

    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class DatabaseWarning(Warning):
    """Warnings about inconsistencies or ambiguities and how they are resolved.

    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class Range():
    """A range of values from min to max.

    """
    def __init__(self, l1, l2=None):
        """Initializes a Range object.

        Args:
            l1:  Minimum value or list/tuple of min and max values.
            l2:  Maximum value of None if l1 is list or tuple.
        """
        if l2 is None:
            self.minval = l1[0]
            self.maxval = l1[1]
        else:
            self.minval = l1
            self.maxval = l2

    def __str__(self):
        return "Range({}, {})".format(self.minval, self.maxval)

    def limits(self):
        """Returns tuple of minimum and maximum values.

        """
        return (self.minval, self.maxval)

    def within(self, x):
        """Returns boolean that is True iff value x is in the range.

        """
        return (x >= self.minval) and (x <= self.maxval)

    def __eq__(self, other):
        return (other.minval == self.minval) and (other.maxval == self.maxval)

    def intersection(self, other):
        """Returns intersection of range with other intersection.

        """
        if self.minval > other.maxval or self.maxval < other.minval:
            return None
        new_max = min(self.maxval, other.maxval)
        new_min = max(self.minval, other.minval)
        return Range(new_min, new_max)


class Switch():
    """Class for holding mutiplexor switches.

    """
    def __init__(self):
        """Initializes Switch object.

        """
        self._e = []

    def append(self, range, sig):
        """Append mutiplexed signal and its range of switch values.

        Args:
            range(Range): The range of switch values.
            sig(Signal) : The signal that is multiplexed.
        """
        for e in self._e:
            if range == e[0] and sig == e[1]:
                return
        self._e.append((range, sig))

    def __len__(self):
        return len(self._e)

    def __str__(self):
        string = ""
        for e in self._e:
            string += str(e[0]) + " -->\n"
            sigstr = str(e[1])
            string += textwrap.indent(sigstr, 4*' ')
        return string

    def any_multiples(self):
        """Returns boolean True iff there is a signal with more than one
        multiplexor range.

        """
        d = {}
        for _, sig in self._e:
            if sig in d:
                d[sig] += 1
            else:
                d[sig] = 1
        for sig in d:
            if d[sig] > 1:
                return True
        return False

    def dbc_sg_mul_val_strs(self):
        d = {}
        sigs = []
        for e in self._e:
            sig = e[1]
            signame = sig.name
            r = e[0]
            if signame in d:
                d[signame].append(r)
            else:
                d[signame] = [r]
                sigs.append(sig)
        for signame in d:
            ranges = d[signame]
            rstrs = [str(r.minval) + '-' + str(r.maxval) for r in ranges]
            d[signame] = ' '.join(rstrs)
        return sigs, d


class Signal():
    """Class for CAN-bus signals.

    """
    def __init__(self, d):
        """Initializes Signal object.

        Args:
           d(dict): Dictionary of parameters.
        """
        self.name = d['name']
        self.multiplex_value = d['multiplex_value']  # None if not multiplexed
        self.is_multiplexor = d['is_multiplexor']
        self.is_little_endian = d['little_endian']
        self.is_signed = d['signed']
        self.start_bit = d['start']
        self.numbits = d['size']
        self.factor = d['factor']
        self.offset = d['offset']
        self.range = Range(d['range'])  # Range of values
        self.unit = d['unit']
        self.receivers = d['receivers']
        self.switch = Switch()
        self.comments = []
        self.attributes = {}
        self.value_descriptions = {}
        self.value_type = None

    def __str__(self):
        if self.numbits == 1:
            string = "{}, {} bit at bit {}".format(self.name,
                                                   self.numbits,
                                                   self.start_bit)
        else:
            string = "{}, {} bits starting at bit {}".format(self.name,
                                                             self.numbits,
                                                             self.start_bit)
        if self.value_type == 0:
            string += ', integer'
        elif self.value_type == 1:
            string += ', float'
        elif self.value_type == 2:
            string += ', double'
        elif self.value_type is not None:
            string += ', value type = {}'.format(self.value_type)
        string += "\n"
        if self.comments:
            for c in self.comments:
                string += c + "\n"
        if self.attributes:
            string += "attributes=" + str(self.attributes) + "\n"
        if self.value_descriptions:
            string += "value descriptions: " + str(self.value_descriptions)\
                + "\n"
        if self.switch:
            string += str(self.switch)
        return string

    def multiplexes(self, val):
        """Return boolean True if signal is a multiplexor for value.

        """
        return self.is_multiplexor\
            and (val >= 0) and (val < 2**self.numbits)

    def dbc(self):
        """Returns dbc string of signal.

        """
        string = "SG_ {} ".format(self.name)
        one_more = False
        if self.multiplex_value is not None:
            string += "m{}".format(self.multiplex_value)
            one_more = True
        if self.is_multiplexor:
            string += "M"
            one_more = True
        if one_more:
            string += ' '
        string += ":"
        string += " {}|{}@".format(self.start_bit, self.numbits)
        if self.is_little_endian:
            string += '1'
        else:
            string += '0'
        if self.is_signed:
            string += '-'
        else:
            string += '+'
        string += " ({},{})".format(self.factor, self.offset)
        string += " [{}|{}]".format(self.range.minval, self.range.maxval)
        string += " \"{}\"  {}".format(self.unit, self.receivers[0])
        for r in self.receivers[1:]:
            string += ", {}".format(r)
        string += "\n"
        return string

    def dbc_sg_mul_val(self):
        strings = []
        if not self.is_multiplexor:
            return ''
        sigs, d = self.switch.dbc_sg_mul_val_strs()
        for sig in sigs:
            strings.append(sig.name + ' ' + self.name + ' ' + d[sig.name])
            strings += sig.dbc_sg_mul_val()
        return strings

    def diff(self, other):
        """Returns string descriptions of difference between signal
        and other signal.

        """
        self_dbc = self.dbc()
        other_dbc = other.dbc()
        if self_dbc != other_dbc:
            return " < {} > {}".format(self.dbc(), other.dbc())
        return ''


class SignalGroup(list):
    """Class of signal groups associated with a message.

    """
    def __init__(self, n, *args, **kwargs):
        """Initialize signal group object.

        Args:
             n(int): Repetitions.
        """
        super().__init__(*args, **kwargs)
        self.n = n

    def __str__(self):
        string = "{} = ".format(self.n)
        for signame in self:
            string += signame

    def dbc(self):
        """Return dbc string for signal group.

        """
        string = "{} :".format(self.n)
        for signame in self:
            string += " " + signame
        string += ';'
        return string

    def diff(self, other):
        """Return description of difference from other signal group.

        """
        if set(self) != set(other):
            return "< {}\n> {}".format(self.dbc(), other.dbc())
        return ''


class Message():
    """Class for can-bus messages.

    """
    def __init__(self, id, name, size, transmitter):
        """Initializes Message object.

        Args:
            id(int):          Message id.
            name(str):        Name of message.
            size(int):        Number of bytes in message.
            transmitter(str)  Name of transmitting node.
        """
        self.id = id
        self.name = name
        self.size = size
        self.transmitters = [transmitter]
        self.signals = []
        self.signals_dict = {}
        self.comments = []
        self.attributes = {}
        self.signal_groups = {}

    def append(self, sg):
        """Append signal to messsage.

        Args:
            sg(Signal): Signal object to append.
        """
        self.signals.append(sg)
        self.signals_dict[sg.name] = sg

    def __str__(self):
        string = "{} {}, {} bytes".format(self.id, self.name, self.size)
        if self.transmitters:
            string += ', transmitters: ' + ' '.join(self.transmitters)
        string += "\n"
        for c in self.comments:
            string += 4*" " + c + "\n"
        if self.attributes:
            string += 4*" " + "attributes=" + str(self.attributes) + "\n"
        if self.signal_groups:
            string += 4*" " + "signal_gropus=" + str(self.signal_groups) + "\n"
        if self.signals:
            sig_string = "signals:\n"
            sub_sig_string = ""
            for s in self.signals:
                sub_sig_string += str(s) + "\n"
            sig_string += textwrap.indent(sub_sig_string, 4*' ')
            string += textwrap.indent(sig_string, 4*" ")
        return string

    def dbc(self):
        """Return dbc-statement for message.

        """
        string = "BO_ {} {}: {} {}\n".format(self.id, self.name, self.size,
                                             self.transmitters[0])
        for signame in self.signals_dict:
            sig = self.signals_dict[signame]
            string += " " + sig.dbc()
        return string

    def dbc_sg_mul_val(self):
        sigstr = ""
        sd = self.signals_dict
        multiple_muxes = len([s for s in sd if sd[s].is_multiplexor]) > 1
        multiple_switches = False
        for s in sd:
            multiple_switches = multiple_switches\
                or sd[s].switch.any_multiples()
        if multiple_muxes or multiple_switches:
            for s in self.signals:
                sstrs = s.dbc_sg_mul_val()
                for sstr in sstrs:
                    sigstr += "SG_MUL_VAL_ {} {};\n".format(self.id, sstr)
        return sigstr

    def diff(self, other):
        """Return string describing difference from other message.

        """
        # id
        if self.id != other.id:
            return "message id:\n < {}\n > {}\n".format(self.id, other.id)
        # name
        if self.name != other.name:
            return "id {} message name:\n < {}\n > {}\n".format(self.id,
                                                                self.name,
                                                                other.name)
        # size
        if self.size != other.size:
            return "id {} message size:\n < {}\n > {}\n".format(self.id,
                                                                self.size,
                                                                other.size)
        # transmitters = [transmitter, ...]
        own_tx = set(self.transmitters)
        other_tx = set(other.transmitters)
        unique_own = own_tx - other_tx
        unique_other = other_tx - own_tx
        if unique_own or unique_other:
            return "id {} transmitters :\n < {}\n > {}\n".format(
                self.id, unique_own, unique_other)
        # signals_dict
        own_sigs = set(self.signals_dict.keys())
        other_sigs = set(other.signals_dict.keys())
        unique_own = own_sigs - other_sigs
        unique_other = other_sigs - own_sigs
        if unique_own or unique_other:
            return "message id {} signals :\n < {}\n > {}\n".format(
                self.id, unique_own, unique_other)
        for signame in self.signals_dict:
            sig_self = self.signals_dict[signame]
            sig_other = other.signals_dict[signame]
            string = sig_self.diff(sig_other)
            if string:
                return "message id {} signal {}:\n{}\n".format(self.id,
                                                               signame,
                                                               string)
        # message comments
        if self.comments != other.comments:
            return "id {} comments:\n < {}\n > {}\n".format(
                self.id, self.comments, other.comments)
        # attributes
        if self.attributes != other.attributes:
            return "id {} attributes:\n < {}\n > {}\n".format(
                self.id, self.attributes, other.attributes)
        # signal_groups
        own_groups = set(self.signal_groups.keys())
        other_groups = set(other.signal_groups.keys())
        unique_own = own_groups - other_groups
        unique_other = other_groups - own_groups
        if unique_own or unique_other:
            return "id {} signal groups :\n < {}\n > {}\n".format(
                self.id, unique_own, unique_other)
        for groupname in self.signal_groups:
            group_self = self.signal_groups[groupname]
            group_other = other.signal_groups[groupname]
            string = group_self.diff(group_other)
            if string:
                return "id {} signal group {} :\n  {}\n".format(
                    self.id, groupname, string)
        # done
        return ''


class Bus():
    """Class containing all information on CAN-bus.

    This class can hold all information that is contained in a
    dbc-file.
    """
    def _exception_handler(self, exception_type, exception, traceback):
        if exception_type == DatabaseError:
            print("{}: {}".format(exception_type.__name__, exception))
        else:
            self._old_excepthook(exception_type, exception, traceback)

    def __init__(self, seclist0, usermode=True, debugmode=None):
        """Initializes Bus object.

        Args:
            seclist0(list):  List of tuples containing parsed sections.
            usermode(bool):  If True, trace of error messages are not shown.
            debugmode(bool): If True, print parsed sections as
                             they are processed
        """
        self._usermode = usermode
        if self._usermode:
            self._old_excepthook = sys.excepthook
            sys.excepthook = self._exception_handler
        if debugmode is None:
            self._debugmode = not self._usermode
        else:
            self._debugmode = debugmode
        seclist = seclist0.copy()
        # *** mandatory sections
        # VERSION  ('VERSION', version_string)
        sl = self._xtract_sec(seclist, 'VERSION')
        if len(sl) == 0:
            raise DatabaseError("missing section: \"VERSION\"")
        elif len(sl) > 1:
            raise DatabaseError("more than one section of type \"VERSION\"")
        if self._debugmode:
            print(sl[0])
        self.version = sl[0][1]
        # BS_ ('BS_', None) | or ('BS_', (baudrate_int, btr1_int, btr2_int)
        sl = self._xtract_sec(seclist, 'BS_')
        if len(sl) == 0:
            raise DatabaseError("missing section: \"BS_\"")
        elif len(sl) > 1:
            raise DatabaseError("more than one section of type \"BS_\"")
        if self._debugmode:
            print(sl[0])
        t = sl[0]
        if t[1]:
            self.baudrate = t[1][0]
            self.btr = t[1][1:]
        else:
            self.baudrate = None
            self.btr = None
        # BU_ ('BU_', nodenames_list)
        sl = self._xtract_sec(seclist, 'BU_')
        if len(sl) == 0:
            raise DatabaseError("missing section: \"BU_\"")
        elif len(sl) > 1:
            raise DatabaseError("more than one section of type \"BU_\"")
        if self._debugmode:
            print(sl[0])
        nl = sl[0][1]
        not_unique = len(nl) - len(set(nl))
        if not_unique:
            warnings.warn("BU_: repeated nodes, removing duplicates",
                          DatabaseWarning)
            nl = list(dict.fromkeys(nl))
        self.nodes = {}
        for node in nl:
            self.nodes[node] = {'comments': [], 'attributes': {}}
        # *** optional sections
        # NS_ ('NS_', new_symbols_list)
        sl = self._xtract_sec(seclist, 'NS_')
        if len(sl) == 0:
            self.newsymbols = []
        elif len(sl) > 1:
            raise DatabaseError("more than one section of type \"NS_\"")
        else:
            self.newsymbols = sl[0][1].copy()
        if self._debugmode:
            print(sl[0])
        # VAL_TABLE_ ('VAL_TABLE_', name_string, table_list) table (int str)*
        self.global_values = {}
        sl = self._xtract_sec(seclist, 'VAL_TABLE_')
        for tab in sl:
            tabname = tab[1]
            if tabname in self.global_values:
                raise DatabaseError("multiply defined table \"{}\"".format(
                    tabname))
            vals = {}
            tab[2].sort(key=lambda x: x[0], reverse=True)
            for t in tab[2]:
                if t[0] in vals:
                    warnings.warn("table \"{}\" has value {} defined "
                                  "more than once, last definition is "
                                  "used".format(tabname, t[0]),
                                  DatabaseWarning)
                vals[t[0]] = t[1]
            self.global_values[tabname] = vals
            if self._debugmode:
                print(tab)
        # BO_ ('BO_', d_dict)
        #     d['id'] = message_id
        #     d['name'] = message_name
        #     d['size'] = size
        #     d['transmitter'] = transmitter
        #     d['signals'] = list_signals
        ml = self._xtract_sec(seclist, 'BO_')
        self.messages = {}
        for _, msg_d in ml:
            if self._debugmode:
                print(('BO_', msg_d))
            msg = Message(msg_d['id'], msg_d['name'], msg_d['size'],
                          msg_d['transmitter'])
            if msg.id in self.messages:
                raise DatabaseError("multiple definitions of "
                                    "message {} {} ".format(msg.id, msg.name))
            self.messages[msg.id] = msg
            for sig_d in msg_d['signals']:
                msg.append(Signal(sig_d))
        # BO_TX_BU_ ('BO_TX_BU_', id_int, transmitters_list_of_identifiers)
        ml = self._xtract_sec(seclist, 'BO_TX_BU_')
        self.comments = []
        for bo in ml:
            if self._debugmode:
                print(bo)
            id = bo[1]
            if id in self.messages:
                msg = self.messages[id]
            else:
                raise DatabaseError("undefined message id \"{}\" in BO_TX_BU-"
                                    "statement".format(id))
            for tx in bo[2]:
                if tx not in self.nodes:
                    raise DatabaseError("transmitter \"{}\" not among "
                                        "defined nodes".format(tx))
                if tx not in msg.transmitters:
                    msg.transmitters.append(tx)
        # EV_ not implemented
        # ENVVAR_DATA_ not implemented
        # SGTYPE_ not implemented
        #
        # CM_ ('CM_', ('BU_', name | 'BO_', message_id
        #           | 'SG_', message_id, name | 'EV_', name
        #           | '' , comment_string))
        cl = self._xtract_sec(seclist, 'CM_')
        for c0 in cl:
            if self._debugmode:
                print(c0)
            c = c0[1]
            ct = c[0]
            if ct == '':
                self.comments.append(c[1])
            elif ct == 'BU_':
                name = c[1]
                if name in self.nodes:
                    self.nodes[name]['comments'].append(c[2])
                else:
                    raise DatabaseError("comment for undefined node \"{}\"",
                                        name)
            elif ct == 'BO_':
                msg_id = c[1]
                if msg_id in self.messages:
                    self.messages[msg_id].comments.append(c[2])
                else:
                    raise DatabaseError("comment for undefined message \"{}\"".
                                        format(msg_id))
            elif ct == 'SG_':
                msg_id = c[1]
                name = c[2]
                if msg_id in self.messages:
                    msg = self.messages[msg_id]
                    if name in msg.signals_dict:
                        msg.signals_dict[name].comments.append(c[3])
                    else:
                        raise DatabaseError("comment for undefined signal "
                                            "\"{}\" in message \"{}\"".format(
                                                name, msg_id))
                else:
                    raise DatabaseError("comment for signal \"{}\" in "
                                        "undefined message \"{}\"".format(
                                            name, msg_id))
            elif ct == 'EV_':
                raise DatabaseError("CM_ EV_ not implented")
            else:
                raise ValueError("unknown comment type specifier "
                                 "\"{}\"".format(ct))
        # BA_DEF_  ('BA_DEF_', spec, name, typ)
        #    spec = 'BU_' | 'BO_' | 'SG_' | 'EV_' | '')
        self.attrib_typedefs = {'': {}, 'BU_': {}, 'BO_': {},
                                'SG_': {}, 'EV_': {}}
        cl = self._xtract_sec(seclist, 'BA_DEF_')
        for c0 in cl:
            if self._debugmode:
                print(c0)
            ct = c0[1]
            name = c0[2]
            typ = c0[3]
            if ct not in self.attrib_typedefs:
                raise DatabaseError("unknown object type \"{}\" in BA_DEF_".
                                    format(ct))
            if name in self.attrib_typedefs[ct]:
                raise DatabaseError("attribute \"{}\" already defined "
                                    "for \"{}\"".format(name, ct))
            if typ[0] in ["INT", "HEX", "FLOAT"]:
                self.attrib_typedefs[ct][name] = {'type': typ[0],
                                                  'values': typ[1:]}
            elif typ[0] == 'STRING':
                self.attrib_typedefs[ct][name] = {'type': typ[0],
                                                  'values': None}
            elif typ[0] == 'ENUM':
                self.attrib_typedefs[ct][name] = {'type': typ[0],
                                                  'values': typ[1]}
            else:
                raise ValueError("unknown object type \"{}\"".format(typ[0]))
        # BA_DEF_DEF_ ('BA_DEF_DEF_', name_identifier, val)
        self.attrib_defaults = {}
        cl = self._xtract_sec(seclist, 'BA_DEF_DEF_')
        for t, name, val in cl:
            if self._debugmode:
                print((t, name, val))
            if name in self.attrib_defaults:
                raise DatabaseError("attribute default value for \"{}\" "
                                    "multiply defined", format(name))
            self.attrib_defaults[name] = val
        # BA_  ('BA_', name_identifier, desc, val)
        #   desc = ('BU_', nodename | 'BO_', message_id
        #         | 'SG_', message_id, name | 'EV_', name | '' )
        self.attributes = {}
        cl = self._xtract_sec(seclist, 'BA_')
        for c in cl:
            if self._debugmode:
                print(c)
            name = c[1]
            d = c[2]
            val = c[3]
            ct = d[0]
            if ct == '':
                if name in self.attributes:
                    raise DatabaseError("general attribute \"{}\" multiply "
                                        "defined ".format(name))
                self.attributes[name] = val
            elif ct == 'BU_':
                node = d[1]
                if node not in self.nodes:
                    raise DatabaseError("unknown node \"{}\" in attribute "
                                        "value statement".format(node))
                nda = self.nodes[node]['attributes']
                if name in nda:
                    raise DatabaseError("attribute \"{}\" multiply defined "
                                        "for node \"{}\"".format(name, node))
                nda[name] = val
            elif ct == 'BO_':
                id = d[1]
                if id not in self.messages:
                    raise DatabaseError("unknown message id \"{}\" in "
                                        "attribute value statement".format(id))
                msg = self.messages[id]
                if name in msg.attributes:
                    raise DatabaseError("attribute \"{}\" multiply defined for"
                                        " message \"{}\"".format(name, id))
                msg.attributes[name] = val
            elif ct == 'SG_':
                id = d[1]
                signame = d[2]
                if id not in self.messages:
                    raise DatabaseError("unknown message id \"{}\" in "
                                        "attribute value statement for "
                                        "signal \"{}\"".format(id, sig))
                sigs = self.messages[id].signals_dict
                if signame not in sigs:
                    raise DatabaseError("unknown message - signal desgination "
                                        "\"{}\" - \"{}\" in attribute value "
                                        "statement".format(id, sig))
                attr = sigs[signame].attributes
                if name in attr:
                    raise DatabaseError("attribute \"{}\" multiply defined for"
                                        " signal \"{}\" in message \"{}\"".
                                        format(name, sig, id))
            elif ct == 'EV_':
                raise DatabaseError("attributes for EV_ not implemented")
            else:
                ValueError("unknown object identifier \"{}\" in attribute "
                           "statement".format(ct))
        # VAL_ ('VAL_', (message_id, sig_name), vals)
        cl = self._xtract_sec(seclist, 'VAL_')
        for c in cl:
            if self._debugmode:
                print(c)
            id = c[1][0]
            signame = c[1][1]
            if id not in self.messages:
                raise DatabaseError("unknown message id \"{}\" in "
                                    "signal value description for "
                                    "signal \"{}\"".format(id, signame))
            sigs = self.messages[id].signals_dict
            if signame not in sigs:
                raise DatabaseError("unknown message - signal designation "
                                    "\"{}\" - \"{}\" in signal value "
                                    "description".format(id, signame))
            for val, desc in c[2]:
                sigs[signame].value_descriptions[val] = desc
        # SIG_GROUP_ ('SIG_GROUP_', msg_id, name, number, sigs)
        cl = self._xtract_sec(seclist, 'SIG_GROUP_')
        for c in cl:
            if self._debugmode:
                print(c)
            id = c[1]
            group_name = c[2]
            if id not in self.messages:
                raise DatabaseError("unknown message id \"{}\" in "
                                    "definition of signal group \"{}\"".
                                    format(id, group_name))
            msg = self.messages[id]
            if group_name in msg.signal_groups:
                raise DatabaseError("signal group \"{}\" already defined for "
                                    "message \"{}\"".format(goup_name, id))
            signals = []  # preserve order but drop duplicates
            for signame in c[4]:
                if signame not in signals:
                    signals.append(signame)
            undef = list(set(signals) - set(msg.signals_dict))
            if undef:
                errstr = "undefined signals in definition of group " +\
                    groupname + " for message \"{}\": ".format(id)\
                    + undef[0]
                for s in undef[1:]:
                    errstr += ', ' + s
                raise DatabaseError(errstr)
            msg.signal_groups[group_name] = SignalGroup(c[3], signals)
        # SIG_VALTYPE_ ( 'SIG_VALTYPE_' message_id, signal_name, tn)
        cl = self._xtract_sec(seclist, 'SIG_VALTYPE_')
        for c in cl:
            if self._debugmode:
                print(c)
            id = c[1]
            signame = c[2]
            if id not in self.messages:
                raise DatabaseError("unknown message id \"{}\" in "
                                    "signal value-type statement for "
                                    "signal \"{}\"".format(id, signame))
            sigs = self.messages[id].signals_dict
            if signame not in sigs:
                raise DatabaseError("unknown message - signal desgination "
                                    "\"{}\" - \"{}\" in signal value-type "
                                    "statement".format(id, signame))
            sigs[signame].value_type = c[3]
        # for messages with only one multiplexor, organize accordingly
        for id in self.messages:
            msg = self.messages[id]
            mux = None
            multiplexed = []
            num_muxes = 0
            for sig in msg.signals:
                if sig.is_multiplexor:
                    num_muxes += 1
                    mux = sig
                if sig.multiplex_value is not None:
                    multiplexed.append(sig)
            if num_muxes == 1:  # organize into hierarchy
                for sig in multiplexed:
                    if mux.multiplexes(sig.multiplex_value):
                        r = Range(sig.multiplex_value, sig.multiplex_value)
                        mux.switch.append(r, sig)
                    else:
                        raise DatabaseError("multiplex value for signal \"{}\""
                                            " in message \"{}\" is not in "
                                            "range of multiplexor \"{}\"".
                                            format(sig.name, msg.id, mux.name))
                    msg.signals.remove(sig)
        # SG_MUL_VAL_ ('SG_MUL_VAL_', (message_id, signal_name,
        #                                     multiplexor_name), ranges)
        cl = self._xtract_sec(seclist, 'SG_MUL_VAL_')
        for c in cl:
            if self._debugmode:
                print(c)
            id = c[1][0]
            signame = c[1][1]
            muxname = c[1][2]
            ranges = c[2]
            if id not in self.messages:
                raise DatabaseError("unknown message id \"{}\" in "
                                    "extended multiplexing statement for "
                                    "signal \"{}\" and mux \"{}\" ".format(
                                        id, signame, muxname))
            msg = self.messages[id]
            if signame not in msg.signals_dict:
                raise DatabaseError("unknown signal name \"{}\" in "
                                    "extended multiplexing statement for "
                                    "message id \"{}\"".format(signame, id))
            sig = msg.signals_dict[signame]
            if muxname not in msg.signals_dict:
                raise DatabaseError("unknown multiplexor name \"{}\" in "
                                    "extended multiplexing statement for "
                                    "message id \"{}\"".format(muxname, id))
            mux = msg.signals_dict[muxname]
            if not mux.is_multiplexor:
                raise DatabaseError("named multiplexor \"{}\" in "
                                    "extended multiplexing statement for "
                                    "message id \"{}\" is not a multiplexor".
                                    format(muxname, id))
            if sig in msg.signals:
                msg.signals.remove(sig)
                for r0 in ranges:
                    r = Range(r0)
                    mux.switch.append(r, sig)
            else:
                raise DatabaseError("signal \"{}\" in message \"{}\" "
                                    "multiplexed be more than one "
                                    "multiplexor".format(signame, id))
        nonmuxed = []
        for id in self.messages:
            msg = self.messages[id]
            for sig in msg.signals:
                if sig.multiplex_value is not None:
                    nonmuxed.append((msg, sig))
        if nonmuxed:  # do this for now, later maybe assign to toplevel mux
            s = "there were signals with unspecified multiplexor: "
            for t in nonmuxed:
                s += "\n    \"{}\": \"{}\"".format(t[0].id, t[1].name)
            raise DatabaseError(s)
        # if not everything is unpacked
        if seclist:
            raise Exception("sections left to unpack: "+str(seclist))
        # *** final touch
        if self._usermode:
            sys.excepthook = self._old_excepthook

    def _xtract_sec(self, seclist, sec):
        indices = [kk for kk in range(len(seclist)) if seclist[kk][0] == sec]
        indices.sort(reverse=True)
        xtr = []
        for ii in indices:
            xtr.append(seclist.pop(ii))
        xtr.reverse()
        return xtr

    ########################################################################
    def dbc(self):
        """Returns dbc-format representation of Bus.

        """
        string = ""
        # VERSION self.version
        string += "VERSION " + "\"" + self.version + "\"\n\n"
        # new symbols
        string += "NS_ :\n"
        for s in self.newsymbols:
            string += "    {}\n".format(s)
        string += "\n"
        # baudrate and timing
        string += "BS_:"
        if self.baudrate and self.btr:
            string += " {}: {}, {}".format(self.baudrate,
                                           self.btr[0], self.btr[1])
        string += "\n\n"
        # nodes
        # BU_  self.nodes[node] = {'comments': [], 'attributes': {}}
        string += "BU_:"
        for node in self.nodes:
            string += " " + node
        string += "\n\n"
        # VAL_TABLE_ self.global_values dict
        for tab in self.global_values:
            string += "VAL_TABLE_ " + tab
            d = self.global_values[tab]
            table = [(n, d[n]) for n in d]
            table.sort(key=lambda x: x[0], reverse=True)
            for t in table:
                string += " {} \"{}\"".format(*t)
            string += " ;\n"
        if self.global_values:
            string += "\n"
        # messages
        for msgid in self.messages:
            string += self.messages[msgid].dbc()
        if self.messages:
            string += "\n"
        # transmitter list
        count = 0
        for msgid in self.messages:
            msg = self.messages[msgid]
            if len(msg.transmitters) > 1:
                count += 1
                string += "BO_TX_BU_ {}:".format(msgid)
                for tx in msg.transmitters:
                    string += " " + tx
                string += " ;\n"
        if count:
            string += "\n"
        # environment_variables environment_variables_data signal_types
        # not implemented
        # comments
        count = 0
        for c in self.comments:
            count += 1
            ce = c.replace("\"", "\\\"")
            string += "CM_ \"{}\";\n".format(ce)
        for node in self.nodes:
            cl = self.nodes[node]['comments']
            for c in cl:
                count += 1
                ce = c.replace("\"", "\\\"")
                string += "CM_ BU_ {} \"{}\";\n".format(node, ce)
        for msgid in self.messages:
            msg = self.messages[msgid]
            for c in msg.comments:
                count += 1
                ce = c.replace("\"", "\\\"")
                string += "CM_ BO_ {} \"{}\";\n".format(msgid, ce)
            d = msg.signals_dict
            for signame in d:
                sig = d[signame]
                for c in sig.comments:
                    count += 1
                    ce = c.replace("\"", "\\\"")
                    string += "CM_ SG_ {} {} \"{}\";\n".format(msgid,
                                                               signame, ce)
        # EV_ comments not implemented
        if count:
            string += "\n"
        # attribute definitions
        # self.attrib_typedefs = {'': {}, 'BU_': {}, 'BO_': {},
        #                        'SG_': {}, 'EV_': {}}
        # keys 'type', 'values'
        count = 0
        for ot in self.attrib_typedefs:
            d = self.attrib_typedefs[ot]
            for attrname in d:
                tdef = d[attrname]
                count += 1
                if len(ot):
                    string += "BA_DEF_ {} ".format(ot)
                else:
                    string += "BA_DEF_ "
                typ = tdef['type']
                val = tdef['values']
                string += "\"" + attrname + "\" " + typ + " "
                if typ in ["INT", "HEX", "FLOAT"]:
                    string += "{} {}".format(val[0], val[1])
                elif typ == "ENUM":
                    string += ", ".join(["\"{}\"".format(s) for s in val])
                string += ";\n"
        if count:
            string += "\n"
        # attribute defaults
        count = 0
        for a in self.attrib_defaults:
            count += 1
            val = self.attrib_defaults[a]
            if isinstance(val, str):
                val = "\"" + val + "\""
            string += "BA_DEF_DEF_ \"" + a + "\" " + str(val) + ";\n"
        if count:
            string += "\n"
        # attribute values
        count = 0
        for aname in self.attributes:
            count += 1
            val = self.attributes[aname]
            if isinstance(val, str):
                string += "BA_ \"{}\" \"{}\";\n".format(aname, val)
            else:
                string += "BA_ \"{}\" {};\n".format(aname, val)
        for node in self.nodes:
            for aname in self.nodes[node]['attributes']:
                count += 1
                val = self.nodes[node]['attributes'][aname]
                if isinstance(val, str):
                    string += "BA_ \"{}\" BU_ {} \"{}\";\n".format(
                        aname, node, val)
                else:
                    string += "BA_ \"{}\" BU_ {} {};\n".format(aname,
                                                               node, val)
        for msgid in self.messages:
            msg = self.messages[msgid]
            for aname in msg.attributes:
                count += 1
                val = msg.attributes[aname]
                if isinstance(val, str):
                    string += "BA_ \"{}\" BO_ {} \"{}\";\n".format(aname,
                                                                   msgid, val)
                else:
                    string += "BA_ \"{}\" BO_ {} {};\n".format(aname,
                                                               msgid, val)
            for signame in msg.signals_dict:
                sig = msg.signals_dict[signame]
                for aname in sig.attributes:
                    count += 1
                    val = sig.attributes[aname]
                    if isinstance(val, str):
                        string += "BA_ \"{}\" SG_ {} {} \"{}\";\n".format(
                            aname, msgid, signame, val)
                    else:
                        string += "BA_ \"{}\" SG_ {} {} {};\n".format(
                            aname, msgid, signame, val)
        if count:
            string += "\n"
        # signal value descriptions
        count = 0
        for msgid in self.messages:
            msg = self.messages[msgid]
            for signame in msg.signals_dict:
                sig = msg.signals_dict[signame]
                if sig.value_descriptions:
                    count += 1
                    d = sig.value_descriptions
                    tab = [(n, d[n]) for n in d]
                    tab.sort(key=lambda x: x[0], reverse=True)
                    string += "VAL_ {} {}".format(msgid, signame)
                    for t in tab:
                        string += " {} \"{}\"".format(*t)
                    string += " ;\n"
        if count:
            string += "\n"
        #
        # signal groups
        count = 0
        for msgid in self.messages:
            msg = self.messages[msgid]
            for gname in msg.signal_groups:
                count += 1
                string += "SIG_GROUP_ {} {} ".format(msgid, gname)\
                    + msg.signal_groups[gname].dbc() + "\n"
        if count:
            string += "\n"
        # extended multiplexing
        count = 0
        for msgid in self.messages:
            msg = self.messages[msgid]
            string += msg.dbc_sg_mul_val()
        # all done
        return string

    def _new_symbols(self):
        string = ""
        string += "CM_\n"
        string += "BA_DEF_\n"
        string += "BA_\n"
        string += "VAL_\n"
        string += "CAT_DEF_\n"
        string += "CAT_\n"
        string += "FILTER\n"
        string += "BA_DEF_DEF_\n"
        string += "EV_DATA_\n"
        string += "ENVVAR_DATA_\n"
        string += "SGTYPE_\n"
        string += "SGTYPE_VAL_\n"
        string += "BA_DEF_SGTYPE_\n"
        string += "BA_SGTYPE_\n"
        string += "SIG_TYPE_REF_\n"
        string += "VAL_TABLE_\n"
        string += "SIG_GROUP_\n"
        string += "SIG_VALTYPE_\n"
        string += "SIGTYPE_VALTYPE_\n"
        string += "BO_TX_BU_\n"
        string += "BA_DEF_REL_\n"
        string += "BA_REL_\n"
        string += "BA_DEF_DEF_REL_\n"
        string += "BU_SG_REL_\n"
        string += "BU_EV_REL_\n"
        string += "BU_BO_REL_\n"
        string += "SG_MUL_VAL_\n"
        return string

    ########################################################################
    def __str__(self):
        string = ""
        # VERSION self.version
        string += "VERSION " + self.version + "\n"
        # comments
        for c in self.comments:
            string += c + "\n"
        # attributes
        if self.attributes:
            string += "attributes:\n"
            for a in self.attributes:
                string += 4*" " + a + " = {}".format(self.attributes[a])\
                    + "\n"
        # BS_ self.baudrate: self.btr[0], self.btr[1]
        if self.baudrate is not None:
            string += "baudrate={}, btr1={}, btr2={}\n".format(self.baudrate,
                                                               self.btr[0],
                                                               self.btr[1])
        # BU_  self.nodes[node] = {'comments': [], 'attributes': {}}
        if self.nodes:
            string += "nodes:\n"
        for node in self.nodes:
            string += 4*" " + node + "\n"
            comments = self.nodes[node]['comments']  # list
            for c in comments:
                string += 8*" " + c + "\n"
            attributes = self.nodes[node]['attributes']  # dict
            if attributes:
                string += " ".join([attr + "=".format(attributes[attr])
                                    for attr in attributes]) + "\n"
        # *** optional sections
        # NS_ not printed
        # VAL_TABLE_ self.global_values dict
        if self.global_values:
            string += "global value tables\n"
        for tab in self.global_values:
            string += 4*" " + tab + " = " + str(self.global_values[tab]) + "\n"
        # BO_
        if self.messages:
            string += "messages:\n"
        msglist = ""
        for msgid in self.messages:
            msglist += str(self.messages[msgid])
        string += textwrap.indent(msglist, 4*' ')
        # EV_ not implemented
        # ENVVAR_DATA_ not implemented
        # SGTYPE_ not implemented
        #
        # BA_DEF_  not displayed
        # BA_DEF_DEF_ not displayed
        # BA_ printed where they belong, EV_ not implemented
        # VAL_ sigs[signame].value_descriptions
        # SIG_GROUP_ handled in msg str method
        # SIG_VALTYPE_  handled in signal str-method
        # SG_MUL_VAL_  handled in signal str-method
        return string

    ################################################################
    def diff(self, other):
        """Return text describing difference in content from other Bus object.

        """
        # not checked:
        #     self._usermode
        #     self._old_excepthook
        #     self._debugmode
        #
        # version
        if self.version != other.version:
            return "version:\n < {}\n > {}\n".format(self.version,
                                                     other.version)
        # baudrate
        if self.baudrate != other.baudrate:
            return "baudrate:\n < {}\n > {}\n".format(self.baudrate,
                                                      other.baudrate)
        # btr
        if self.btr != other.btr:
            if self.btr[0] != other.btr[0]:
                return "btr1:\n < {}\n > {}\n".format(self.btr[0],
                                                      other.btr[0])
            if self.btr[1] != other.btr[1]:
                return "btr2:\n < {}\n > {}\n".format(self.btr[1],
                                                      other.btr[1])
        # nodes
        own_nodes = set(self.nodes.keys())
        other_nodes = set(other.nodes.keys())
        unique_own = own_nodes - other_nodes
        unique_other = other_nodes - own_nodes
        if unique_own or unique_other:
            return "nodes:\n < {}\n > {}\n".format(unique_own, unique_other)
        string = "nodes:\n"
        count = 0
        for k in self.nodes:
            if self.nodes[k] != other.nodes[k]:
                count += 1
                string += "   {}:\n" "      < {}\n"\
                    "      > {}\n".format(k, self.nodes[k], other.nodes[k])
        if count:
            return string
        # new symbols
        own_symbols = set(self.newsymbols)
        other_symbols = set(other.newsymbols)
        unique_own = own_symbols - other_symbols
        unique_other = other_symbols - own_symbols
        if unique_own or unique_other:
            return "new symbols:\n < {}\n > {}\n".format(unique_own,
                                                         unique_other)
        # global values
        if self.global_values != other.global_values:
            return "global values:\n < {}\n > {}\n".format(self.global_values,
                                                           other.global_values)
        # messages
        own_ids = set(self.messages.keys())
        other_ids = set(other.messages.keys())
        unique_own = own_ids - other_ids
        unique_other = other_ids - own_ids
        if unique_own or unique_other:
            return "messages by id:\n < {}\n > {}\n".format(unique_own,
                                                            unique_other)
        for msgid in self.messages:
            string = self.messages[msgid].diff(other.messages[msgid])
            if string:
                return string
        # comments
        if self.comments != other.comments:
            return "global comments:\n < {}\n > {}\n".format(self.comments,
                                                             other.comments)
        # attribute definitions
        if self.attrib_typedefs != other.attrib_typedefs:
            return "attribute definitions:\n < {}\n > {}\n".format(
                self.attrib_typedefs, other.attrib_typedefs)

        # attribute defaults
        if self.attrib_defaults != other.attrib_defaults:
            return "attribute defaults:\n < {}\n > {}\n".format(
                self.attrib_defaults, other.attrib_defaults)

        # attributes
        if self.attributes != other.attributes:
            return "attributes:\n < {}\n > {}\n".format(self.attributes,
                                                        other.attributes)
        # done
        return ''


class ParseError(Exception):
    """Syntax errors during parsing of dbc-file.

    """
    def __init__(self, line, col, msg, *args):
        """Initializes ParseError object.

        Reports failure to parse string.

        Args:
            line(int) Line number of error.
            col(int): Column number of error.
            msg(str): Informative description of error.
            args:     Variable arguments
        """
        self.line = line
        self.col = col
        self.msg = msg
        self.args = args

    def __str__(self):
        s = self.msg.format(*self.args)
        return "{} line {}, column {}".format(s, self.line,
                                              self.col)


class Parser:
    """Class for dbc parsers.

    """
    keywords = ('VERSION', 'NS_', 'NS_DESC_', 'CM_', 'BA_DEF_',
                'BA_', 'VAL_', 'CAT_DEF_', 'CAT_', 'FILTER', 'BA_DEF_DEF_',
                'EV_DATA_', 'ENVVAR_DATA_', 'SGTYPE_', 'SGTYPE_VAL_',
                'BA_DEF_SGTYPE_', 'BA_SGTYPE_', 'SIG_TYPE_REF_', 'VAL_TABLE_',
                'SIG_GROUP_', 'SIG_VALTYPE_', 'SIGTYPE_VALTYPE_', 'BO_TX_BU_',
                'BA_DEF_REL_', 'BA_REL_', 'BA_DEF_DEF_REL_', 'BU_SG_REL_',
                'BU_EV_REL_', 'BU_BO_REL_',  'SG_MUL_VAL_', 'BS_', 'BU_',
                'BO_', 'SG_', 'EV_', 'VECTOR__INDEPENDENT_SIG_MSG',
                'Vector__XXX')

    _ = list(keywords)
    _.remove("VECTOR__INDEPENDENT_SIG_MSG")
    keywords_BO = tuple(_)

    _ = list(keywords)
    _.remove('Vector__XXX')
    keywords_most = tuple(_)
    del _

    sections = (  # order made to give first match on longest possibe keyword
        'BA_DEF_DEF_', 'BA_DEF_', 'BA_',
        'BO_TX_BU_', 'BO_', 'BS_', 'BU_',
        'CM_', 'ENVVAR_DATA_', 'EV_', 'NS_',
        'SIG_GROUP_', 'SIG_TYPE_REF_', 'SIG_VALTYPE_',
        'SGTYPE_', 'SG_MUL_VAL_',
        'VAL_TABLE_', 'VAL_', 'VERSION',
    )

    def __init__(self, usermode=True):
        """Initalize Parser object.

        Args:
            usermode(bool): If True, suppress error message traces.
        """
        self._usermode = usermode
        self.stack = []

    def _exception_handler(self, exception_type, exception, traceback):
        if exception_type == ParseError:
            print("{}: {}".format(exception_type.__name__, exception))
        else:
            self._old_excepthook(exception_type, exception, traceback)

    def parse(self, text):
        """Parse set up parser and parse dbc text.

        Args:
            text(str):  String containing dbc file.
        """
        if self._usermode:
            self._old_excepthook = sys.excepthook
            sys.excepthook = self._exception_handler
        self.text = text
        self.n = 0
        self.len = len(text)
        self.col = 0
        self.line = 1
        self.stack = []
        value = self.start()
        self.assert_at_end()
        if self._usermode:
            sys.excepthook = self._old_excepthook
        return value

    def getpos(self):
        """Return current position within string.

        The position is a tuple of character, line and column number.
        """
        return (self.n, self.line, self.col)

    def setpos(self, pos):
        """Set current position within string.

        Args: pos(tuple): Character, line and column number.
        """
        self.n, self.line, self.col = pos

    def assert_at_end(self):
        if self.n < self.len:
            raise ParseError(self.line, self.col,
                             "Unrecognizable text remains from")

    def eat_whitespace(self):
        while self.n < self.len and (c := self.text[self.n]) in " \f\v\r\t\n":
            self.n += 1
            if c == ' ':
                self.col += 1
            if c == '\n':
                self.col = 0
                self.line += 1

    def charmatch(self, c):
        if self.n >= self.len:
            raise ParseError(self.line, self.col,
                             "Reached end while looking for \"{}\" at", c)
        if self.text[self.n] == c:
            self.n += 1
            self.col += 1
            if c == '\n':
                self.col = 0
                self.line += 1
        else:
            raise ParseError(self.line, self.col, "Expected \"{}\", found "
                             "\"{}\" at", c, self.text[self.n])

    def strmatch(self, s):
        if self.text[self.n:].startswith(s):
            self.n += len(s)
            self.col += len(s)
            return s
        raise ParseError(self.line, self.col,
                         "Expected \"{}\", found \"{}\" at ",
                         s, self.text[self.n:self.n+len(s)])

    def uint(self):
        n = self.n
        s = ''
        while n < self.len:
            if (c := self.text[n]) in '0123456789':
                s += c
                n += 1
            else:
                break
        if s:
            self.col += n - self.n
            self.n = n
            return(int(s))
        raise ParseError(self.line, self.col, "Expected unsigned int, "
                         "found \"{}\" at", self.text[self.n:self.n+10])

    def double(self):
        pattern = r'^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?'
        match = re.match(pattern, self.text[self.n:])
        if match:
            span = match.span()
            val = float(self.text[self.n:self.n + span[1]])
            self.n += span[1]
            self.col += span[1]
            return val
        raise ParseError(self.line, self.col, "Expected a floating point, "
                         "found \"{}\" at", self.text[self.n:self.n+10])

    def sint(self):
        pattern = r'^[-+]?[0-9]+'
        match = re.match(pattern, self.text[self.n:])
        if match:
            span = match.span()
            val = int(self.text[self.n:self.n + span[1]])
            self.n += span[1]
            self.col += span[1]
            return val
        raise ParseError(self.line, self.col, "Expected a signed integer, "
                         "found \"{}\" at", self.text[self.n:self.n+10])

    def eat_pattern(self, pattern):
        match = re.match(pattern, self.text[self.n:])
        if match:
            span = match.span()
            self.n += span[1]
            self.col += span[1]
            nln = self.text[self.n:].count('\n', span[0], span[1])
            if nln:
                self.line += nln
                self.col = span[1] - 1\
                    - self.text[self.n:].rfind('\n', span[0], span[1])

    def string(self):
        if self.text[self.n] != "\"":
            raise ParseError(self.line, self.col,
                             "expected \'\\{}\' but found \'{}\' at ", "\"",
                             self.text[self.n])
        self.n += 1
        self.col += 1
        s = ''
        while self.n < self.len:
            c = self.text[self.n]
            if c == '\n':
                self.line += 1
                self.col = 0
            elif c == '\\':
                raise ParseError(self.line, self.col,
                                 "Encounted backslash in string at ")
            self.n += 1
            self.col += 1
            if c == '\"':
                break
            else:
                s += c
        else:
            raise ParseError(self.line, self.col,
                             "Reached end while parsing string")
        return s

    def idchar(self, c):
        o = ord(c)
        return (o >= ord('a') and o <= ord('z'))\
            or (o >= ord('A') and o <= ord('Z'))\
            or (o >= ord('0') and o <= ord('9'))\
            or c == '_'

    def identifier(self, reserved=[]):
        n = self.n
        n0 = n
        s = ''
        while n < self.len:
            if self.idchar(c := self.text[n]):
                n += 1
            else:
                break
        str = self.text[n0:n]
        if str.isidentifier() and str not in reserved:
            self.n = n
            self.col += n-n0
            return str
        if str in reserved:
            raise ParseError(self.line, self.col,
                             "Identifier equals reserved word \"{}\" at",
                             str)
        raise ParseError(self.line, self.col, "Expected identifier, "
                         "but found \"{}\" at", str)

    def identifier_ws(self, reserved):
        res = self.identifier(reserved)
        self.eat_whitespace()
        return res

    def identifier_list(self, reserved, sep_pattern="^[ ]*"):
        idl = []
        res = self.optional(lambda: self.identifier(reserved))
        if res is None:
            return idl
        idl.append(res)
        while self.n < self.len:
            pos = self.getpos()
            self.eat_pattern(sep_pattern)
            res = self.optional(lambda: self.identifier(reserved))
            if res:
                idl.append(res)
            else:
                self.setpos(pos)
                break
        return idl

    def string_list(self, sep_pattern="^[ ]*,[ ]*"):
        idl = []
        res = self.optional(self.string)
        if res is None:
            return idl
        idl.append(res)
        while self.n < self.len:
            pos = self.getpos()
            self.eat_pattern(sep_pattern)
            res = self.optional(self.string)
            if res:
                idl.append(res)
            else:
                self.setpos(pos)
                break
        return idl

    def optional(self, rule):
        pos = self.getpos()
        try:
            return rule()
        except ParseError:
            self.setpos(pos)
            return None

    def any_number_of(self, rule):
        res = []
        while self.n < self.len:
            pos = self.getpos()
            try:
                res0 = rule()
            except ParseError:
                self.setpos(pos)
                break
            res.append(res0)
        return res

    def one_of(self, *rules):
        ln = 0
        cn = 0
        ex = None
        for rule in rules:
            pos = self.getpos()
            try:
                res = rule()
                return res
            except ParseError as pe:
                self.setpos(pos)
                if (ln0 := pe.line) > ln or (ln0 == ln and cn < pe.col):
                    ln = pe.line
                    cn = pe.col
                    ex = pe
        raise ex from None

    def section_body_VERSION(self):
        self.eat_whitespace()
        return ('VERSION', self.string())

    def section_body_NS_(self):
        symnames = ('BA_DEF_DEF_REL_', 'BA_DEF_DEF_', 'BA_DEF_SGTYPE_',
                    'BA_DEF_REL_', 'BA_DEF_', 'BA_REL_', 'BA_SGTYPE_', 'BA_',
                    'BO_TX_BU_', 'BU_SG_REL_', 'BU_EV_REL_', 'BU_BO_REL_',
                    'CM_', 'CAT_DEF_', 'CAT_',
                    'ENVVAR_DATA_', 'EV_DATA_', 'FILTER',
                    'NS_DESC_',
                    'SIG_GROUP_', 'SIG_TYPE_REF_', 'SIG_VALTYPE_',
                    'SIGTYPE_VALTYPE_', 'SGTYPE_VAL_', 'SGTYPE_',
                    'SG_MUL_VAL_',
                    'VAL_TABLE_', 'VAL_'
                    )
        self.eat_whitespace()
        if (c := self.text[self.n]) != ':':
            raise ParseError(self.line, self.col,
                             "Expected \":\", found \"{}\" at", c)
        self.n += 1
        self.col += 1
        self.eat_whitespace()

        list = []
        while self.n < self.len:
            self.eat_whitespace()
            if self.text[self.n] == ':':
                if list:
                    list.pop()
                    self.setpos(stored_pos)
                    break
                else:
                    raise ParseError(self.line, self.col,
                                     "Expected reserved word, "
                                     "found \":\" at")
            stored_pos = self.getpos()
            s = self.identifier()
            if s in symnames:
                list.append(s)
            elif s in Parser.keywords:
                self.setpos(stored_pos)
                break
        return ('NS_', list)

    def baudrate(self):
        baudrate = self.uint()
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        btr1 = self.uint()
        self.eat_whitespace()
        self.charmatch(',')
        self.eat_whitespace()
        btr2 = self.uint()
        return (baudrate, btr1, btr2)

    def section_body_BS_(self):
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        res = self.optional(self.baudrate)
        return ('BS_', res)

    def section_body_BU_(self):
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        return ('BU_', self.any_number_of(lambda:
                self.identifier_ws(reserved=Parser.keywords_most)))

    def value_entry(self):
        self.eat_whitespace()
        value = self.sint()
        self.eat_whitespace()
        text = self.string()
        return (value, text)

    def section_body_VAL_TABLE_(self):
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        self.eat_whitespace()
        table = self.any_number_of(self.value_entry)
        self.eat_whitespace()
        self.charmatch(';')
        return ('VAL_TABLE_', name, table)

    def multiplex_value(self):
        self.charmatch('m')
        self.eat_whitespace()
        return self.uint()

    def multiplex_spec(self):
        mval = self.optional(self.multiplex_value)
        self.eat_whitespace()
        try:
            self.charmatch('M')
            multiplexor = True
        except ParseError:
            multiplexor = False
        return (mval, multiplexor)

    def endian(self):
        c = self.text[self.n]
        if c == '1':
            self.n += 1
            self.col += 1
            return True
        elif c == '0':
            self.n += 1
            self.col += 1
            return False
        else:
            ParseError(self.line, self.col, "Expected \"0\" or \"1\", but "
                       "found \"{}\" at", c)

    def signed(self):
        c = self.text[self.n]
        if c == '+':
            self.n += 1
            self.col += 1
            return False
        elif c == '-':
            self.n += 1
            self.col += 1
            return True
        else:
            ParseError(self.line, self.col, "Expected \"+\" or \"-\", but "
                       "found \"{}\" at", c)

    def additional_receiver(self):
        self.eat_whitespace()
        self.charmatch(',')
        self.eat_whitespace()
        return self.identifier(Parser.keywords_most)

    def signal(self):
        self.eat_whitespace()
        self.strmatch('SG_')
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        self.eat_whitespace()
        multi = self.multiplex_spec()
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        start = self.uint()
        self.eat_whitespace()
        self.charmatch('|')
        self.eat_whitespace()
        size = self.uint()
        self.eat_whitespace()
        self.charmatch('@')
        self.eat_whitespace()
        little = self.endian()
        self.eat_whitespace()
        signed_value = self.signed()
        self.eat_whitespace()
        self.charmatch('(')
        self.eat_whitespace()
        factor = self.double()
        self.eat_whitespace()
        self.charmatch(',')
        self.eat_whitespace()
        offset = self.double()
        self.eat_whitespace()
        self.charmatch(')')
        self.eat_whitespace()
        self.charmatch('[')
        self.eat_whitespace()
        minimum = self.double()
        self.eat_whitespace()
        self.charmatch('|')
        self.eat_whitespace()
        maximum = self.double()
        self.eat_whitespace()
        self.charmatch(']')
        self.eat_whitespace()
        unit = self.string()
        self.eat_whitespace()
        receivers = [self.identifier(Parser.keywords_most)] +\
            self.any_number_of(self.additional_receiver)
        d = {}
        d['name'] = name
        d['multiplex_value'] = multi[0]
        d['is_multiplexor'] = multi[1]
        d['start'] = start
        d['size'] = size
        d['little_endian'] = little
        d['signed'] = signed_value
        d['factor'] = factor
        d['offset'] = offset
        d['range'] = (minimum, maximum)
        d['unit'] = unit
        d['receivers'] = receivers
        return d

    def section_body_BO_(self):
        self.eat_whitespace()
        message_id = self.uint()
        self.eat_whitespace()
        message_name = self.identifier(Parser.keywords_BO)
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        size = self.uint()
        self.eat_whitespace()
        transmitter = self.identifier(Parser.keywords_most)
        self.eat_whitespace()
        signals = self.any_number_of(self.signal)
        d = {}
        d['id'] = message_id
        d['name'] = message_name
        d['size'] = size
        d['transmitter'] = transmitter
        d['signals'] = signals
        return ('BO_', d)

    def section_body_BO_TX_BU_(self):
        self.eat_whitespace()
        id = self.uint()
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        transmitters = self.identifier_list(Parser.keywords_most, "^[ ,]*")
        self.eat_whitespace()
        self.charmatch(';')
        return ('BO_TX_BU_', id, transmitters)

    def cm_specifier_BU_(self):
        self.strmatch('BU_')
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        return ('BU_', name)

    def cm_specifier_BO_(self):
        self.strmatch('BO_')
        self.eat_whitespace()
        message_id = self.uint()
        return ('BO_', message_id)

    def cm_specifier_SG_(self):
        self.strmatch('SG_')
        self.eat_whitespace()
        message_id = self.uint()
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        return ('SG_', message_id, name)

    def cm_specifier_EV_(self):
        self.strmatch('EV_')
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        return ('EV_', name)

    def section_body_CM_(self):
        self.eat_whitespace()
        spec = self.one_of(self.cm_specifier_SG_,
                           self.cm_specifier_BU_,
                           self.cm_specifier_BO_,
                           self.cm_specifier_EV_,
                           lambda: ('',))
        self.eat_whitespace()
        spec += (self.string(),)
        self.eat_whitespace()
        self.charmatch(';')
        return ('CM_', spec)

    def sig_val_type_spec(self):
        if (c := self.text[self.n]) in '0123':
            self.n += 1
            self.col += 1
            return int(c)
        raise ParseError(self.line, self.col, "Expected one of \"0123\", "
                         "found \"{}\" at", c)

    def section_body_SIG_VALTYPE_(self):
        self.eat_whitespace()
        message_id = self.uint()
        self.eat_whitespace()
        signal_name = self.identifier(Parser.keywords)
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        tn = self.sig_val_type_spec()
        self.eat_whitespace()
        self.charmatch(';')
        return ('SIG_VALTYPE_', message_id, signal_name, tn)

    def uint_range(self):
        low = self.uint()
        self.eat_whitespace()
        self.charmatch('-')
        self.eat_whitespace()
        high = self.uint()
        return (low, high)

    def sep_uint_range(self, sep_char_pattern):
        self.eat_pattern(sep_char_pattern)
        return self.uint_range()

    def uint_ranges(self, sep_char_pattern):
        ranges = []
        self.eat_whitespace()
        ranges.append(self.uint_range())
        ranges += self.any_number_of(lambda:
                                     self.sep_uint_range(sep_char_pattern))
        return ranges

    def section_body_SG_MUL_VAL_(self):
        self.eat_whitespace()
        message_id = self.uint()
        self.eat_whitespace()
        signal_name = self.identifier(Parser.keywords)
        self.eat_whitespace()
        multiplexor_name = self.identifier(Parser.keywords)
        self.eat_whitespace()
        ranges = self.optional(lambda: self.uint_ranges(r'^[ ,]*'))
        if not ranges:
            ranges = []
        self.eat_whitespace()
        self.charmatch(';')
        return ('SG_MUL_VAL_', (message_id, signal_name, multiplexor_name),
                ranges)

    def section_body_VAL_(self):
        self.eat_whitespace()
        message_id = self.uint()
        self.eat_whitespace()
        sig_name = self.identifier(Parser.keywords)
        self.eat_whitespace()
        vals = self.any_number_of(self.value_entry)
        self.eat_whitespace()
        self.charmatch(';')
        return ('VAL_', (message_id, sig_name), vals)

    def ba_int(self):
        self.strmatch('INT')
        self.eat_whitespace()
        val1 = self.sint()
        self.eat_whitespace()
        val2 = self.sint()
        return ('INT', val1, val2)

    def ba_hex(self):
        self.strmatch('HEX')
        self.eat_whitespace()
        val1 = self.sint()
        self.eat_whitespace()
        val2 = self.sint()
        return ('HEX', val1, val2)

    def ba_float(self):
        self.strmatch('FLOAT')
        self.eat_whitespace()
        val1 = self.double()
        self.eat_whitespace()
        val2 = self.double()
        return ('FLOAT', val1, val2)

    def ba_string(self):
        self.strmatch('STRING')
        return ('STRING',)

    def ba_enum(self):
        self.strmatch('ENUM')
        self.eat_whitespace()
        strs = self.string_list()
        return ('ENUM', strs)

    def section_body_BA_DEF_(self):
        self.eat_whitespace()
        spec = self.one_of(lambda: self.strmatch('BU_'),
                           lambda: self.strmatch('BO_'),
                           lambda: self.strmatch('SG_'),
                           lambda: self.strmatch('EV_'),
                           lambda: '')
        self.eat_whitespace()
        self.charmatch("\"")
        name = self.identifier(Parser.keywords)
        self.charmatch("\"")
        self.eat_whitespace()
        typ = self.one_of(self.ba_float, self.ba_int, self.ba_hex,
                          self.ba_string, self.ba_enum)
        self.eat_whitespace()
        self.strmatch(';')
        return ('BA_DEF_', spec, name, typ)

    def section_body_BA_DEF_DEF_(self):
        self.eat_whitespace()
        self.charmatch("\"")
        name = self.identifier(Parser.keywords)
        self.charmatch("\"")
        self.eat_whitespace()
        val = self.one_of(self.uint, self.sint, self.double, self.string)
        self.eat_whitespace()
        self.charmatch(';')
        return ('BA_DEF_DEF_', name, val)

    def desc_BA_BU_(self):
        self.strmatch('BU_')
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        return ('BU_', name)

    def desc_BA_BO_(self):
        self.strmatch('BO_')
        self.eat_whitespace()
        id = self.uint()
        return ('BO_', id)

    def desc_BA_SG_(self):
        self.strmatch('SG_')
        self.eat_whitespace()
        id = self.uint()
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        return ('SG_', id, name)

    def desc_BA_EV_(self):
        self.strmatch('EV_')
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        return ('EV_', name)

    def section_body_BA_(self):
        self.eat_whitespace()
        name = self.string()
        self.eat_whitespace()
        desc = self.one_of(self.desc_BA_BU_, self.desc_BA_BO_,
                           self.desc_BA_SG_, self.desc_BA_EV_,
                           lambda: ('',))
        self.eat_whitespace()
        val = self.one_of(self.double, self.uint, self.sint, self.string)
        self.eat_whitespace()
        self.charmatch(";")
        return ('BA_', name, desc, val)

    def section_body_SIG_GROUP_(self):
        self.eat_whitespace()
        msg_id = self.uint()
        self.eat_whitespace()
        name = self.identifier(Parser.keywords)
        self.eat_whitespace()
        number = self.uint()
        self.eat_whitespace()
        self.charmatch(':')
        self.eat_whitespace()
        sigs = self.any_number_of(lambda: self.identifier_ws(Parser.keywords))
        self.eat_whitespace()
        self.charmatch(';')
        return ('SIG_GROUP_', msg_id, name, number, sigs)

    def section_keyword(self):
        for str in Parser.sections:
            ll = len(str)
            if self.text[self.n:self.n+ll] == str:
                self.n += ll
                self.col += ll
                return str
        strex = self.text[self.n:self.n+10]
        raise ParseError(self.line, self.col,
                         "Expected section keyword but found instead "
                         "\"{}\" at", strex.replace("\"", "\\\""))

    def section(self):
        name = self.section_keyword()
        rule = getattr(self, "section_body_" + name, None)
        if rule is None:
            raise ParseError(self.line, self.col,
                             "Unimplemented section type {} encountered near",
                             name)
        return rule()

    def start(self):
        """Parse until empty string.

        """
        secs = []
        self.eat_whitespace()
        while self.n < self.len:
            res = self.section()
            if res is not None:
                secs.append(res)
            else:
                break
            self.eat_whitespace()
        return secs
