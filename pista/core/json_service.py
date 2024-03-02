import json
import re

import numpy as np
import pandas as pd
from jsonpath_ng import parse

from core.file_service import ExcelUtil, JsonUtil, DataGeneric


class NpEncoder(json.JSONEncoder):  # Numpy encoder for datatypes
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.bool_):
            return bool(obj)
        # if isinstance(obj, np.NaN):
        #     return None
        return super(NpEncoder, self).default(obj)


def _ordered_set(input_list):
    seen = set()
    return [x for x in input_list if not (x in seen or seen.add(x))]


def _col_index_in_worksheet(column: str, worksheet, maxcol: int):
    """Find data column index"""
    # for c in range(3, maxcol + 1):
    #     if worksheet.cell(1, c).value == column:
    #         col_index = c
    #         break
    col_index = next((c for c in range(3, maxcol + 1) if worksheet.cell(1, c).value == column), None)
    assert col_index is not None, f"Column {column} not found in sheet {worksheet}"
    return col_index


def _get_all_json_path_by_val(jsonDict, val_starts_with):
    json_path_list = []
    jsonPath_val_dict = {}
    jsonpath_expr = parse("$..*")  # Parse the json data using jsonpath expression

    for match in jsonpath_expr.find(jsonDict):  # Iterate over the matches and check if the value matches the value
        if f"{match.value}".startswith(val_starts_with):
            xpath = str(match.full_path).replace('.[', '[') if '.[' in str(match.full_path) else str(match.full_path)
            json_path_list.append(xpath)  # Print the full path of the match
            jsonPath_val_dict[xpath] = f"{match.value}"

    return json_path_list, jsonPath_val_dict


def _build_final_dict_format(worksheet, maxrow: int):
    """Generate dict format"""
    final_dict = {}
    for r in range(2, maxrow + 1):
        xpath, tag = worksheet.cell(r, 1).value, worksheet.cell(r, 2).value
        if xpath is not None:
            if '.' in xpath:
                _nested_dict_build_by_path(final_dict, xpath, tag)
            else:
                final_dict.update({xpath: {}})
    assert len(final_dict) > 0, f"Final json dict not built"
    return final_dict


def _final_dict_update_val(final_dict, worksheet, maxrow: int, col_index: int):
    """Update dict with values"""
    rootParentTag = worksheet.cell(2, 1).value

    prevParentTag = rootParentTag
    for r in range(2, maxrow + 1):
        xpath, tag, value = worksheet.cell(r, 1).value, worksheet.cell(r, 2).value, worksheet.cell(r, col_index).value
        if xpath is not None:
            calc_xpath = prevParentTag = xpath
        else:
            calc_xpath = prevParentTag
        _nested_dict_update_val_by_path(final_dict, calc_xpath, tag, value)

        
def _nested_dict_build_by_path(nested_dict, key_path, tag):
    keys = key_path.split('.')  # split the path by the separator

    '''Handle until last key'''
    prev_arr_index = None
    for key in keys[:-1]:  # loop through the keys except the last one
        is_currKey_list = True if '[' in key else False
        currArr_index = key[key.find("[") + 1:key.find("]", key.find("["))] if is_currKey_list and key.find(
            "[") + 1 != key.find("]") else '' if is_currKey_list else None
        calc_currArr_index = 0 if currArr_index is None or currArr_index == '' else int(currArr_index)
        calc_currKey = key.replace(f'[{currArr_index}]', '') if is_currKey_list else key

        if calc_currKey not in nested_dict:  # if the key is not in the dict, create a new dict for it
            nested_dict[calc_currKey] = [] if is_currKey_list else {}
            if is_currKey_list and len(nested_dict[calc_currKey]) < calc_currArr_index + 1:
                nested_dict[calc_currKey].append({})
        nested_dict = nested_dict[calc_currKey]  # move to the next level of the dict
        prev_arr_index = calc_currArr_index

    '''Handle last key'''
    lastKey = keys[-1]
    is_lastKey_list = True if '[' in lastKey else False
    lastArr_index = \
        lastKey[lastKey.find("[") + 1:lastKey.find("]", lastKey.find("["))] if is_lastKey_list and lastKey.find(
            "[") + 1 != lastKey.find("]") else '' if is_lastKey_list else None
    calc_lastArr_index = 0 if lastArr_index is None or lastArr_index == '' else int(lastArr_index)
    calc_lastKey = lastKey.replace(f'[{lastArr_index}]', '') if is_lastKey_list else lastKey

    if is_lastKey_list:
        if calc_lastKey not in nested_dict.keys():
            nested_dict[calc_lastKey] = []
        if len(nested_dict[calc_lastKey]) < calc_lastArr_index + 1 and tag is not None:
            nested_dict[calc_lastKey].append({})
    else:
        if type(nested_dict) == list:
            nested_dict[prev_arr_index][calc_lastKey] = {}
        else:
            nested_dict[calc_lastKey] = {}


def _nested_dict_update_val_by_path(nested_dict, key_path, tag, value):
    """Update nested dict with values"""
    keys = key_path.split('.')  # split the path by the separator

    '''Handle dict value'''
    for key in keys[:]:
        is_currKey_list = True if '[' in key else False
        currArr_index = key[key.find("[") + 1:key.find("]", key.find("["))] if is_currKey_list and key.find(
            "[") + 1 != key.find("]") else '' if is_currKey_list else None
        calc_currArr_index = 0 if currArr_index is None or currArr_index == '' else int(currArr_index)
        calc_currKey = key.replace(f'[{currArr_index}]', '') if is_currKey_list else key

        if is_currKey_list and tag is None:
            nested_dict = nested_dict[calc_currKey]
        else:
            if is_currKey_list:
                nested_dict = nested_dict[calc_currKey][calc_currArr_index]
            else:
                nested_dict = nested_dict[calc_currKey]
    if tag is None:
        if value is not None:
            nested_dict.append(value)
    else:
        nested_dict[tag] = value


def _nested_dict_update_jsondata_by_path(nested_dict, key_path, value):
    keys = key_path.split('.')  # split the path by the separator
    final_value = value

    '''Handle sub-json data'''
    for key in keys[:]:
        is_currKey_list = True if '[' in key else False
        currArr_index = key[key.find("[") + 1:key.find("]", key.find("["))] if is_currKey_list and key.find(
            "[") + 1 != key.find("]") else '' if is_currKey_list else None
        calc_currArr_index = 0 if currArr_index is None or currArr_index == '' else int(currArr_index)
        calc_currKey = key.replace(f'[{currArr_index}]', '') if is_currKey_list else key
        final_value = [i[key] for i in final_value]
        
        if is_currKey_list and key == keys[:][len(keys[:]) - 1]:
            nested_dict[calc_currKey] = final_value
        else:
            nested_dict = nested_dict[calc_currKey]
    # nested_dict = value


def _nested_dict_update_rowlist_by_path(nested_dict, key_path, value):
    keys = key_path.split('.')  # split the path by the separator

    '''Handle rows list'''
    for key in keys[:]:
        is_currKey_list = True if '[' in key else False
        currArr_index = key[key.find("[") + 1:key.find("]", key.find("["))] if is_currKey_list and key.find(
            "[") + 1 != key.find("]") else '' if is_currKey_list else None
        calc_currArr_index = 0 if currArr_index is None or currArr_index == '' else int(currArr_index)
        calc_currKey = key.replace(f'[{currArr_index}]', '') if is_currKey_list else key

        if is_currKey_list and key == keys[:][len(keys[:]) - 1]:
            nested_dict[calc_currKey] = value
        else:
            nested_dict = nested_dict[calc_currKey]
    # nested_dict = value


# def build_nested_dict_by_path(nested_dict, key_path, tag, value, update_val=False, update_rowlist=False):
#     keys = key_path.split('.')  # split the path by the separator
#
#     # if not (update_val or update_rowlist):
#     if not update_val:
#         '''Handle until last key'''
#         prev_arr_index = None
#         for key in keys[:-1]:  # loop through the keys except the last one
#             is_currKey_list = True if '[' in key else False
#             currArr_index = key[key.find("[") + 1:key.find("]", key.find("["))] if is_currKey_list and key.find(
#                 "[") + 1 != key.find("]") else '' if is_currKey_list else None
#             calc_currArr_index = 0 if currArr_index is None or currArr_index == '' else int(currArr_index)
#             calc_currKey = key.replace(f'[{currArr_index}]', '') if is_currKey_list else key
#
#             if calc_currKey not in nested_dict:  # if the key is not in the dict, create a new dict for it
#                 nested_dict[calc_currKey] = [] if is_currKey_list else {}
#                 if is_currKey_list and len(nested_dict[calc_currKey]) < calc_currArr_index + 1:
#                     nested_dict[calc_currKey].append({})
#             nested_dict = nested_dict[calc_currKey]  # move to the next level of the dict
#             prev_arr_index = calc_currArr_index
#
#         '''Handle last key'''
#         lastKey = keys[-1]
#         is_lastKey_list = True if '[' in lastKey else False
#         lastArr_index = \
#             lastKey[lastKey.find("[") + 1:lastKey.find("]", lastKey.find("["))] if is_lastKey_list and lastKey.find(
#                 "[") + 1 != lastKey.find("]") else '' if is_lastKey_list else None
#         calc_lastArr_index = 0 if lastArr_index is None or lastArr_index == '' else int(lastArr_index)
#         calc_lastKey = lastKey.replace(f'[{lastArr_index}]', '') if is_lastKey_list else lastKey
#
#         if is_lastKey_list:
#             if calc_lastKey not in nested_dict.keys():
#                 nested_dict[calc_lastKey] = []
#             if len(nested_dict[calc_lastKey]) < calc_lastArr_index + 1 and tag is not None:
#                 nested_dict[calc_lastKey].append({})
#         else:
#             if type(nested_dict) == list:
#                 nested_dict[prev_arr_index][calc_lastKey] = {}
#             else:
#                 nested_dict[calc_lastKey] = {}
#
#     '''Handle dict value'''
#     if update_val:
#         for key in keys[:]:
#             is_currKey_list = True if '[' in key else False
#             currArr_index = key[key.find("[") + 1:key.find("]", key.find("["))] if is_currKey_list and key.find(
#                 "[") + 1 != key.find("]") else '' if is_currKey_list else None
#             calc_currArr_index = 0 if currArr_index is None or currArr_index == '' else int(currArr_index)
#             calc_currKey = key.replace(f'[{currArr_index}]', '') if is_currKey_list else key
#
#             if is_currKey_list and tag is None:
#                 nested_dict = nested_dict[calc_currKey]
#             else:
#                 if is_currKey_list:
#                     nested_dict = nested_dict[calc_currKey][calc_currArr_index]
#                 else:
#                     nested_dict = nested_dict[calc_currKey]
#         if tag is None:
#             if value is not None:
#                 nested_dict.append(value)
#         else:
#             nested_dict[tag] = value
#
#     '''Handle rows list'''
#     if update_rowlist:
#         for key in keys[:]:
#             is_currKey_list = True if '[' in key else False
#             currArr_index = key[key.find("[") + 1:key.find("]", key.find("["))] if is_currKey_list and key.find(
#                 "[") + 1 != key.find("]") else '' if is_currKey_list else None
#             calc_currArr_index = 0 if currArr_index is None or currArr_index == '' else int(currArr_index)
#             calc_currKey = key.replace(f'[{currArr_index}]', '') if is_currKey_list else key
#
#             if is_currKey_list and tag is None:
#                 nested_dict = nested_dict[calc_currKey]
#             else:
#                 if is_currKey_list:
#                     nested_dict = nested_dict[calc_currKey][calc_currArr_index]
#                 else:
#                     nested_dict = nested_dict[calc_currKey]
#         if tag is None:
#             if value is not None:
#                 nested_dict = value
#         else:
#             nested_dict[tag] = value


class JsonService:

    @staticmethod
    def get_json_from_xl(xlfilepath: str, sheet: str, column: str, isChildJson:bool=None):
        worksheet = ExcelUtil.read_xlsheet(xlfilepath, sheet)
        maxcol, maxrow = worksheet.max_column, worksheet.max_row
        rootParentTag = worksheet.cell(2, 1).value

        assert worksheet.cell(2, 1).value is not None, 'Json root tag missing in excel'
        assert column is not None or column != '', 'Column in json file is not correct'

        col_index = _col_index_in_worksheet(column, worksheet, maxcol)  # Find data column index
        final_dict = _build_final_dict_format(worksheet, maxrow)  # Generate dict format
        _final_dict_update_val(final_dict, worksheet, maxrow, col_index)  # Update dict with values

        if not isChildJson:
            JsonService._json_replace_with_jsonData(final_dict, xlfilepath)
        JsonService._json_replace_with_rowsList(final_dict, xlfilepath)
        print(f"Final dict {final_dict}")

        '''Convert and replace'''
        jsonStr = JsonUtil.get_json_str_from_dict(final_dict, clsEncoder=NpEncoder)
        jsonStr = DataGeneric.replace_dyn(jsonStr)  # replace dyn values
        jsonDict = JsonUtil.get_json_dict_from_str(jsonStr)

        return jsonDict, jsonStr

    @staticmethod
    def _json_replace_with_jsonData(jsonDict, xlfilepath: str):
        if 'json(' not in str(jsonDict):
            print('No json variable required')
        else:
            tagPath_list, tagPath_val_dict = _get_all_json_path_by_val(jsonDict, "json(")

            '''Fetch different data sets'''
            parentPath_list, calc_parentPath_list, tag_list = [], [], []
            parentPath_set, calc_parentPath_set = set(), set()
            parentPath_colIdList_dict, calc_parentPath_colIdList_dict = {}, {}
            parentPath_tagList_dict, calc_parentPath_tagList_dict = {}, {}

            prev_parentPath = None
            for i in tagPath_list:
                parentPath = '.'.join(i.split('.')[:len(i.split('.')) - 1])
                # calc_parentPath = i[:i.find('[')]
                calc_parentPath = re.sub(r'\[\d*\]', '', parentPath)
                tag = i.split('.')[-1]
                columnRef = tagPath_val_dict[f"{parentPath}.{tag}"]
                columnId_list = re.findall(r"\((.*?)\)", columnRef)[0].split(',')
                # tag_list = [] if prev_parentPath is None or prev_parentPath != parentPath else tag_list

                parentPath_list.append(parentPath)
                calc_parentPath_list.append(calc_parentPath)
                # tag_list.append(tag)
                parentPath_colIdList_dict.update({parentPath: columnId_list})
                calc_parentPath_colIdList_dict.update({calc_parentPath: columnId_list})

                # if parentPath not in parentPath_tagList_dict.keys():
                #     parentPath_tagList_dict[parentPath] = tag_list
                #     calc_parentPath_tagList_dict[calc_parentPath] = tag_list
                #
                prev_parentPath = parentPath
                # parentPath_set = _ordered_set(parentPath_list)
                # calc_parentPath_set = _ordered_set(calc_parentPath_list)

            print(parentPath_list, calc_parentPath_list, tag_list)
            print(parentPath_colIdList_dict, calc_parentPath_colIdList_dict)

            '''Fetch cols data'''
            for pp, cpp in zip(parentPath_colIdList_dict, calc_parentPath_colIdList_dict):
                if 'json(' in str(jsonDict):
                    subJson_list = []
                    colSheet = cpp
                    # colHdrNum_dict, rowHdrNum_dict, maxRow = ExcelUtil._get_all_xlheaders_num_dict(xlfilepath, colSheet)
                    # colHdrRange_dict, rowHdrRange_dict, maxRow = ExcelUtil._get_all_xlheaders_numrange_dict(xlfilepath, rowSheet)
                    reqCol_list = calc_parentPath_colIdList_dict[colSheet]
                    for c in reqCol_list:
                        temp_jsonDict, temp_jsonStr = JsonService.get_json_from_xl(xlfilepath, colSheet, c, isChildJson=True)
                        subJson_list.append(temp_jsonDict)
                    _nested_dict_update_jsondata_by_path(jsonDict, pp, subJson_list)
        return jsonDict
    
    @staticmethod
    def _json_replace_with_rowsList(jsonDict, xlfilepath: str):
        if 'rows(' not in str(jsonDict):
            print('No rows variable required')
        else:
            tagPath_list, tagPath_val_dict = _get_all_json_path_by_val(jsonDict, "rows(")

            '''Fetch different data sets'''
            parentPath_list, calc_parentPath_list, tag_list = [], [], []
            parentPath_set, calc_parentPath_set = set(), set()
            parentPath_rowId_dict, calc_parentPath_rowId_dict = {}, {}
            parentPath_tagList_dict, calc_parentPath_tagList_dict = {}, {}

            prev_parentPath = None
            for i in tagPath_list:
                parentPath = '.'.join(i.split('.')[:len(i.split('.')) - 1])
                # calc_parentPath = i[:i.find('[')]
                calc_parentPath = re.sub(r'\[\d*\]', '', parentPath)
                tag = i.split('.')[-1]
                rowRef = tagPath_val_dict[f"{parentPath}.{tag}"]
                rowId = re.findall(r"\((.*?)\)", rowRef)[0]
                tag_list = [] if prev_parentPath is None or prev_parentPath != parentPath else tag_list

                parentPath_list.append(parentPath)
                calc_parentPath_list.append(calc_parentPath)
                tag_list.append(tag)
                parentPath_rowId_dict.update({parentPath: rowId})
                calc_parentPath_rowId_dict.update({calc_parentPath: rowId})

                if parentPath not in parentPath_tagList_dict.keys():
                    parentPath_tagList_dict[parentPath] = tag_list
                    calc_parentPath_tagList_dict[calc_parentPath] = tag_list

                prev_parentPath = parentPath
                parentPath_set = _ordered_set(parentPath_list)
                calc_parentPath_set = _ordered_set(calc_parentPath_list)

            '''Fetch rows data'''
            for pp, cpp in zip(parentPath_set, calc_parentPath_set):
                rowSheet = cpp
                # colHdr_num_dict, rowHdr_num_dict, maxRow = ExcelUtil._get_all_xlheaders_num_dict(xlfilepath, rowSheet)
                colHdrRange_dict, rowHdrRange_dict, maxRow = ExcelUtil._get_all_xlheaders_numrange_dict(xlfilepath, rowSheet)
                rowId = calc_parentPath_rowId_dict[rowSheet]
                start, end = rowHdrRange_dict[rowId]
                k = pp
                v_list = parentPath_tagList_dict[k]

                rowItems = []
                df = ExcelDFUtil.fetch_xl_df_data(xlfilepath, rowSheet)
                for r in range(start, end + 1):  # iterate for each rows
                    rowItem = {}
                    for c in v_list:  # iterate for each cols
                        val = ExcelDFUtil.get_xl_df_val_by_rowIndex_colName(df, r - 2, c)
                        # print(pp, r, c, val)
                        rowItem[c] = val
                    rowItems.append(rowItem)

                '''Call to update jsonDict with rowsList'''
                # build_nested_dict_by_path(jsonDict, pp, None, rowItems, update_rowlist=True)
                _nested_dict_update_rowlist_by_path(jsonDict, pp, rowItems)
        return jsonDict

    # json_service file
    def read_xl_validationsheet_by_rowName(self, xlfilepath, rowName):
        sheet = 'Validation'
        validation_dict = {}

        colHdrRange_dict, rowHdrRange_dict, maxRow = ExcelUtil._get_all_xlheaders_numrange_dict(xlfilepath, sheet)
        start, end = rowHdrRange_dict[rowName]

        df = ExcelDFUtil.fetch_xl_df_data(xlfilepath, sheet)
        maxcol = len(df.columns)
        for c in range(1, maxcol):
            colName = df.columns[c]
            val = ExcelDFUtil.get_xl_df_val_by_rowIndex_colName(df, start - 2, colName)
            validation_dict[colName] = val

        return validation_dict

    @staticmethod
    def get_xl_validation_data_by_rowName(xlfilepath, rowName):
        """Sheet = Validation
        """
        sheet = 'Validation'
        validation_dict = {}

        colHdrRange_dict, rowHdrRange_dict, maxRow = ExcelUtil._get_all_xlheaders_numrange_dict(xlfilepath, sheet)
        start, end = rowHdrRange_dict[rowName]

        df = ExcelDFUtil.fetch_xl_df_data(xlfilepath, sheet)
        maxcol = len(df.columns)
        for c in range(1, maxcol):
            colName = df.columns[c]
            val = ExcelDFUtil.get_xl_df_val_by_rowIndex_colName(df, start - 2, colName)
            # val = val.split(',') if isinstance(val, str) and ',' in val else val
            # val = sorted(val) if type(val) == list else val
            validation_dict[colName] = val

        return validation_dict

class ExcelDFUtil:

    @staticmethod
    def fetch_xl_df_data(xl_file_path, sheet: str = None):
        if sheet is not None:
            df = pd.read_excel(xl_file_path, sheet_name=sheet)
        else:
            df = pd.read_excel(xl_file_path)
        df = df.replace({np.nan: None})
        return df

    @staticmethod
    def get_xl_df_val_by_rowIndex_colName(df, rowIndex, colName):
        val = df.loc[int(rowIndex), colName]
        return val
