# -*- coding: UTF-8 -*-
'''
==============================================================================
Created by Erik P G Johansson 2020-10-13.


Module for generic string functions. Not for printing.


BOGIQ:
------
==============================================================================
'''



import re



def read_token(s, regexp_list, search_dir):
    '''
Intended to be analogous to MATLAB code erikpgjohansson.str.read_token().


Utility function for parsing char string, one regular expression-defined
token at a time from beginning or end.

Useful for complex parsing of strings.
** Ex: Parsing filenames
** Ex: Complete parsing of more complex text file syntaxes, e.g. ODL, CSV
       tables with both quoted and unquoted values.


ALGORITHM
=========
Find the first regexp in (ordered) list of regexps that matches the
beginning/end of the string. Return substring succeeding/preceeding the match.
NOTE: Deliberately does NOT throw exceptions when failing to match regexp so
that the caller can throw customized error messages with location etc.


ARGUMENTS
=========
s          : String
search_dir :  1 : Search forward
           : -1 : Search backward
regex_list : Ordered list of regular expressions.
            NOTE: An algorithm for parsing of text often requires a special
            case for when argument "str" is empty. Note that this is
            distinctly different from a regexp that COULD match an empty
            substring, but DOES NOT HAVE TO cover the entire string, e.g.
            regexp '.*' or 'X*'  (if it was not for maximal munch), or
            '(|word)'. This can be implemented by using the regexp "$", at the
            appropriate location in the list of regexps, and then handle that
            in analogy with other regexp, e.g. by using switch(iRegexp).


RETURN VALUES
=============
(token, remaining_str, i_regexp)
--
token         : Substring from the beginning of s (argument) that matches
                regexp regex_list[i_regexp].
remaining_str : The remainder of argument s.
i_regexp      : Index into varargin for the matching regex.
                -1 if no match was found.
--
NOTE: searchDir== 1 ==> [token, remainingStr] == s
NOTE: searchDir==-1 ==> [remainingStr, token] == s


Created 2020-10-13.
'''
    '''
PROPOSAL: Return string indices to remaining string, not entire remaining string.
    PRO: Potentially faster if parsing long strings, e.g. files.
'''

    if search_dir == 1:
        regexp_modif_func  = lambda s:        '^'+s
        remaining_str_func = lambda s, token: s[len(token):]
    elif search_dir == -1:
        regexp_modif_func  = lambda s:        s+'$'
        # IMPLEMENTATION NOTE: Can not use s[:-len(token)] since it does NOT
        # generalize to len(token)==0. ==> Return empty string instead of entire string.
        remaining_str_func = lambda s, token: s[:len(s)-len(token)]
    else:
        raise Exception('Illegal argument search_dir.')

    for i_regexp in range(len(regexp_list)):

        regexp = regexp_modif_func(regexp_list[i_regexp])
        mo = re.search(regexp, s)
        if mo:
            token = mo.group(0)
            remaining_str = remaining_str_func(s, token)

            return (token, remaining_str, i_regexp)

    return (None, s, -1)



def regexp_str_parts(s, regexp_list, search_dir, nonmatch_policy):
    '''
Intended to be analogous to MATLAB code erikpgjohansson.str.regexp_str_parts().

Split a string into consecutive parts, each one corresponding to a regexp
match.

The primary purpose of this function is to make it easy to parse a string
syntax.


ALGORITHM
=========
The algorithm will try to match the beginning of the string to the first
regexp, then continue to match the remainder of the string for each successive
regexp. Perfect match means that the last regexp matches the then remaining
string exactly.
--
NOTE: The matching can fail in two ways:
(a) Algorithm runs out of string before running out of regular expressions.
(b) Algorithm runs out of regular expressions before running out of string.
--
NOTE: A regexp that does match an empty string (e.g. 'a*'), may return an
empty substring.
--
NOTE: The algorithm does not work for "all" applications.
Ex: Matching with restrictive regexp (one or several) at the end of string
while simultaneously having regexp that permits ~arbitrary string in the
beginning/middle. The arbitrary string will match until the end (maximal
munch), preventing matching the last regular expressions.


ARGUMENTS
=========
s               : String
regexp_list     : Cell array of strings, each one containing a regexp.
                  Always ordered in the same order as the substrings in "s",
                  regardless of search direction.
search_dir      : +1 = forward; -1=backward
nonmatch_policy : String constant determining what happens in the event of a
                  non-perfect match (including no match).
                 'assert match'
                 'permit non-match'
                 This refers to both kinds of failure (above).


RETURN VALUE
============
(substr_list, remaining_str, is_perfect_match)
substr_list      : Cell array of strings, each being a match for the corresponding
                   string in regexpList.
                   Always ordered in the same order as the substrings in "s".
remaining_str    : The remainder of argument str that was not matched.
is_perfect_match : Whether matched all regular expressions to entire
                   string.

Initially created 2020-10-14 by Erik P G Johansson.
    '''

    # Inner function for modifying the return result (multiple locations).
    def create_return_result(substr_list, remaining_str, is_perfect_match):
        substr_list = modify_substr_list(substr_list)
        return (substr_list, remaining_str, is_perfect_match)



    '''================
    Interpret arguments
    ================'''
    if nonmatch_policy == 'assert match':
        assert_match = True
    elif nonmatch_policy == 'permit non-match':
        assert_match = False
    else:
        raise Exception('Illegal argument nonmatch_policy="{}".'.format(
            nonmatch_policy))

    if search_dir == 1:
        modify_substr_list = lambda ssl : ssl
    elif search_dir == -1:
        regexp_list = regexp_list[::-1]
        modify_substr_list = lambda ssl : ssl[::-1]
    else:
        raise Exception('Illegal argument search_dir.')



    '''========
     ALGORITHM
    ========'''
    substr_list   = []
    remaining_str = s
    for i_re in range(len(regexp_list)):
        # IMPLEMENTATION NOTE: Do not confuse
        # i_re, and
        # j_rt_re  (RE=read_token).
        (token, remaining_str, j_rt_re) = read_token(
            remaining_str, [regexp_list[i_re]], search_dir)

        if j_rt_re == -1:
            if assert_match:
                raise Exception(
                    'Failed to match regular expression'
                    +' {0:d}="{1}" in argument string "{2}".'.format(
                        i_re, regexp_list[i_re], s))
            else:
                return create_return_result(substr_list, remaining_str, False)

        substr_list.append(token)

    '''==================================
    Check if algorithm matched everything
    =================================='''
    if remaining_str:
        if assert_match:
            # Bad error message if "s" is very long (e.g. file).
            raise Exception(
                'Could not match entire argument string '+
                '"{0}"'.format(s))
        else:
            return create_return_result(substr_list, remaining_str, False)

    return create_return_result(substr_list, remaining_str, True)
