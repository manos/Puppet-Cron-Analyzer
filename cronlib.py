### -*- coding: utf-8 -*-
# misc functions for handling cron data fields
#
#  supports all legal cron entries in vixie cron (i.e. all linux flavors, os x, solaris, etc)
#
#   Author: Charlie Schluting <charlie@schluting.com>
#
# Usage:
#  call normalize_entry('*/15 * * * * my command') to convert ranges, steps, names, etc into
#  a 6-tuple containing normal integer/list representations for each field.
#
#  use generate_timestamps() to return a list of *all* timestamps a cron will run at, for the next year.
#
#  the expand_* functions translate numbers, ranges, names, etc (of days, months, ...) into
#  a standard format (just numbers), and return a list of integers.
#
# Examples:
'''
import cronlib
cron = "*/10 0 0 0 0 my awesome command, is awesome"
print cronlib.normalize_entry(cron)

 ('0,10,20,30,40,50', '0', '0', '0', '0', 'my awesome command, is awesome')

cron = "0-60/10 */2 0 0 0 mycommand... ..."
print cronlib.normalize_entry(cron)

 ('0,10,20,30,40,50,60', '0,2,4,6,8,10,12,14,16,18,20,22', '0', '0', '0', 'mycommand... ...')

cron = "0-60/10 0 0 0 Mon,Tue command..."
print cronlib.normalize_entry(cron)

 ('0,10,20,30,40,50,60', '0', '0', '0', '1,2', 'command...')

cron = "@monthly my monthly command....."
print cronlib.normalize_entry(cron)

 ('0', '0', '1', '1,2,3,4,5,6,7,8,9,10,11,12', '0,1,2,3,4,5,6', 'my monthly command.....')

'''

import collections

symbolic_names = {
    '@yearly':        '0 0 1 1 *',
    '@annually':      '0 0 1 1 *',
    '@monthly':       '0 0 1 * *',
    '@weekly':        '0 0 * * 0',
    '@daily':         '0 0 * * *',
    '@midnight':      '0 0 * * *',
    '@hourly':        '0 * * * *',
}

all_values = {
    'minutes':   range(0, 60),
    'hours':     range(0, 24),
    'monthdays': range(1, 32),
    'months':    range(1, 13),
    'weekdays':  range(0, 7),
}

chars = ('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')

days_map = { 'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5, 'sat': 6, }

months_map = { 'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6, 'jul': 7,
               'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12, }

# helper functions for expand_ methods:
def _flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in _flatten(el):
                yield sub
        else:
            yield el

def _flatten_intify(l):
    ''' flattens any nested lists, converts each item to an int, and de-dups '''
    l = list(_flatten(l))

    return list(set([int(i) for i in l]))

def _exp_human_name (item):
    ''' if passed a string like Sun, Dec, etc, it translates to the associated
        integer. Returns original string if it can't be translated.
        Ranges/steps are not valid for symbolic names, thankfully. '''

    if item[0] not in chars:
        return item

    entry = item.lower()[:3]

    if entry in days_map:
        return days_map[entry]
    elif entry in months_map:
        return months_map[entry]
    else:
        return item

def _exp_range (item):

    # if it's an int, nothing to do:
    if isinstance(item, int):
        return item

    if '-' not in item:
        return item

    # assume we'll never see a step-range here. Just straight integer ranges.
    a, b = item.split('-')

    # add one to the end range, because cron ranges are inclusive of both values.
    return range(int(a),int(b)+1)

def _exp_step (item, unit):
    ''' parses steps (2-4/2 or */5) into list of integers '''

    # if it's an int, nothing to do:
    if isinstance(item, int):
        return item

    if '/' not in item:
        return item

    if item[0] is '*':
        # '*/2' means every 2 mins (for minute field)

        _step = item.split('/')[1]
        start = all_values[unit][:1][0]
        end   = all_values[unit][-1:][0]

        return range(int(start), int(end)+1, int(_step))

    elif '-' in item:
        # '0-8/2' means [0, 2, 4, 6, 8]

        _range, step = item.split('/')
        a, b = _range.split('-')

        return range(int(a), int(b)+1, int(step))

    else: return item


def _exp_list (item_list, unit):
    ''' this is the entry point for parsing.. if it's a list, start here.
        parses list 2,3,5-11/2 or 2,*/2 into list of integers, depending on the unit:
        minutes, hours, days, months '''

    item_list = item_list.split(',')

    # what happens if we have an item in a list with * by itself? That'd be weird..
    # just return all values.
    if [c for c in item_list if c is '*']:
        return all_values[unit]

    # first things first - we can't evaluate until we've expanded other weirdness.
    # 1. expand human names
    # if it contains characters, convert those:
    item_list = [_exp_human_name(c) for c in item_list]

    # 2. steps: it's possible to have a step within a list.
    item_list = [_exp_step(c, unit) for c in item_list]

    # 3. do we have any ranges left (step-ranges already dealt with)?
    item_list = [_exp_range(c) for c in item_list]

    # 4. we could have a nested list now. flatten, convert to ints, return.

    return _flatten_intify(item_list)


def expand_minute (minute):
    ''' expands a 'minute' cron entry to all minutes it will run.
        returns a list of minutes as integers '''

    if '@' in minute:
        return list(symbolic_names[minute.lower()].split()[0])

    # if it's a list, expand
    # All helpers handle expanding when encountering chars for others, to truly return expanded
    # values. So just call them in order:
    if ',' in minute:
        result =  _exp_list(minute, 'minutes')

    elif '/' in minute:
        result =  _exp_step(minute, 'minutes')

    elif '-' in minute:
        result =  _exp_range(minute)

    elif minute[0] in chars:
        result =  _exp_human_name(minute)

    elif '*' in minute:
        result =  all_values['minutes']

    else: result = minute

    return result

def expand_hour (hour):
    ''' expands an 'hour' cron entry to all hours it will run.
        returns a list of hours as integers '''

    if '@' in hour:
        return list(symbolic_names[hour.lower()].split()[1])

    if ',' in hour:
        result =  _exp_list(hour, 'hours')

    elif '/' in hour:
        result =  _exp_step(hour, 'hours')

    elif '-' in hour:
        result =  _exp_range(hour)

    elif hour[0] in chars:
        result =  _exp_human_name(hour)

    elif '*' in hour:
        result =  all_values['hours']

    else: result = hour

    return result

def expand_monthday (day):
    ''' expands a 'day' cron entry to all days (of the month) it will run.
        returns a list of days as integers '''

    if '@' in day:
        return list(symbolic_names[day.lower()].split()[2])

    if ',' in day:
        result =  _exp_list(day, 'monthdays')

    elif '/' in day:
        result =  _exp_step(day, 'monthdays')

    elif '-' in day:
        result =  _exp_range(day)

    elif day[0] in chars:
        result =  _exp_human_name(day)

    elif '*' in day:
        result =  all_values['monthdays']

    else: result = day

    return result

def expand_month (month):
    ''' expands a 'month' cron entry to all months it will run.
        returns a list of months as integers '''

    if '@' in month:
        return list(symbolic_names[month.lower()].split()[3])

    if ',' in month:
        result =  _exp_list(month, 'months')

    elif '/' in month:
        result =  _exp_step(month, 'months')

    elif '-' in month:
        result =  _exp_range(month)

    elif month[0] in chars:
        result =  _exp_human_name(month)

    elif '*' in month:
        result =  all_values['months']

    else: result = month

    return result

def expand_weekday (day):
    ''' expands a 'day' cron entry to all days (of the week) it will run.
        returns a list of days as integers '''

    if '@' in day:
        return list(symbolic_names[day.lower()].split()[4])

    if ',' in day:
        result =  _exp_list(day, 'weekdays')

    elif '/' in day:
        result =  _exp_step(day, 'weekdays')

    elif '-' in day:
        result =  _exp_range(day)

    elif day[0] in chars:
        result =  _exp_human_name(day)

    elif '*' in day:
        result =  all_values['weekdays']

    else: result = day

    return result


def expand_timestamps (normalized_cron_entry):
    ''' returns a generator object, containing a list of all timestamps a cron
        will run at, for the next year '''
    #TODO
    pass

def normalize_entry (cron_entry):
    ''' Returns a full cron entry as a 6-tuple, but normalized into lists of integers
        for each minute, hour, etc field. From here, it's easy to parse or it can be
        sent to expand_timestamps to get a list of all times it runs, for the next
        year. '''

    # ignore (return False?) if @reboot is used.
    # input: 0 * * * * command with spaces

    cron = cron_entry.split()

    if '@' in cron[0]:
        # @reboot isn't periodic, it's special. We can't deal with that.
        if cron[0].lower() in '@reboot': return False

        new = symbolic_names[cron[0]].split()
        new.append(' '.join(cron[1:]))

        cron = new

    minute   = ','.join(map(str, expand_minute(cron[0])))
    hour     = ','.join(map(str, expand_hour(cron[1])))
    monthday = ','.join(map(str, expand_monthday(cron[2])))
    month    = ','.join(map(str, expand_month(cron[3])))
    weekday  = ','.join(map(str, expand_weekday(cron[4])))
    command  = ' '.join(cron[5:])

    entry =  minute, hour, monthday, month, weekday, command

    return entry


if __name__ == '__main__':
    test()

'''
Mr. Vixie says:

              field          allowed values
              -----          --------------
              minute         0-59
              hour           0-23
              day of month   1-31
              month          1-12 (or names, see below)
              day of week    0-7 (0 or 7 is Sun, or use names)

       A field may be an asterisk (*), which always stands for ''first-last''.

       Ranges  of  numbers  are  allowed.  Ranges are two numbers separated with a hyphen.  The specified range is inclusive.  For example, 8-11 for an ''hours'' entry
       specifies execution at hours 8, 9, 10 and 11.

       Lists are allowed.  A list is a set of numbers (or ranges) separated by commas.  Examples: ''1,2,5,9'', ''0-4,8-12''.

       Step values can be used in conjunction with ranges.  Following a range with ''/<number>'' specifies skips of the number's value through the range.  For example,
       ''0-23/2''   can   be   used   in   the   hours   field   to   specify   command   execution   every   other  hour  (the  alternative  in  the  V7  standard  is
       ''0,2,4,6,8,10,12,14,16,18,20,22'').  Steps are also permitted after an asterisk, so if you want to say ''every two hours'', just use ''*/2''.

       Names can also be used for the ''month'' and ''day of week'' fields.  Use the first three letters of the particular day or month (case doesn't matter).   Ranges
       or lists of names are not allowed.

       Note: The day of a command's execution can be specified by two fields — day of month, and day of week.  If both fields are restricted (i.e., aren't *), the com-
       mand will be run when either field matches the current time.  For example,
       ''30 4 1,15 * 5'' would cause a command to be run at 4:30 am on the 1st and 15th of each month, plus every Friday.

       Instead of the first five fields, one of eight special strings may appear:

              string         meaning
              ------         -------
              @reboot        Run once, at startup.
              @yearly        Run once a year, "0 0 1 1 *".
              @annually      (same as @yearly)
              @monthly       Run once a month, "0 0 1 * *".
              @weekly        Run once a week, "0 0 * * 0".
              @daily         Run once a day, "0 0 * * *".
              @midnight      (same as @daily)
              @hourly        Run once an hour, "0 * * * *".

'''
