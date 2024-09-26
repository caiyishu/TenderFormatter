def ProcessItemRecord(item_record):
    """
    創建代碼企業名稱表
    """
    df_ids = pd.DataFrame(item_record["brief"]["companies"]["ids"])
    if df_ids.shape[0] == 0:
        return  # 跳过空缺标案

    df_names = pd.DataFrame(item_record["brief"]["companies"]["names"])
    if df_names.shape[0] == 0:
        return  # 跳过空缺标案
    df_names.columns = ["names"]
    
    if df_ids.shape[0] != df_names.shape[0]:
        return  # 跳过错误记录

    df_ids_names = pd.concat([df_ids, df_names], axis=1)
    df_ids_names.columns = ["ids", "names"]
    """
    創建基礎信息表
    """
    df_name_key = pd.DataFrame({
        "names":
        item_record["brief"]["companies"]["name_key"].keys(),
        "docs":
        item_record["brief"]["companies"]["name_key"].values(),
    })

    df_name_key = df_name_key.explode("docs")
    df_name_key["docs"] = df_name_key["docs"].fillna("").astype(str)
    df_name_key["status"] = df_name_key["docs"].str.split(":").str[-1]
    df_name_key["item_key"] = df_name_key["docs"].str.split(
        ":").str[:3].str.join(":")
    df_name_key = pd.merge(df_ids_names, df_name_key, on=["names"], how="left")
    df_name_key = df_name_key[df_name_key["status"] != "廠商名稱"]
    df_name_key.drop_duplicates(inplace=True)
    """
    創建詳細信息表
    """
    df_detail = pd.DataFrame({
        "item": item_record["detail"].keys(),
        "item_value": item_record["detail"].values(),
    })
    df_detail["item"] = df_detail["item"].fillna("").astype(str)
    df_detail["item_key"] = df_detail["item"].str.split(":").str[:3].str.join(
        ":")
    df_detail["item_content"] = df_detail["item"].str.split(":").str[-1]
    df_detail = pd.merge(df_name_key, df_detail, on=["item_key"], how="outer")

    df_detail = df_detail[[
        "ids", "names", "item_key", "item_content", "item_value"
    ]]
    """
    获取详情信息
    """
    result_list = []

    # 获取所有公司ID和名称，去除重复和缺失值
    df_ids_names_unique = df_ids_names[["ids",
                                        "names"]].dropna().drop_duplicates()

    # 遍历每个公司，计算所需的数据
    for idx, row in df_ids_names_unique.iterrows():
        company_id = row["ids"]
        company_name = row["names"]

        # 筛选出该公司的数据
        company_data = df_detail[df_detail["ids"] == company_id]

        # 计算总得标数量
        winning_bids = company_data[
            (company_data["item_key"].str.contains("得標廠商", na=False))
            & (company_data["item_content"] == "得標廠商")]
        total_winning_bids = winning_bids["item_key"].nunique()

        # 计算未得标总数量
        losing_bids = company_data[
            (company_data["item_key"].str.contains("未得標廠商", na=False))
            & (company_data["item_content"] == "未得標廠商")]
        total_losing_bids = losing_bids["item_key"].nunique()

        # 计算得标金额总和
        winning_amounts = company_data[
            (company_data["item_key"].str.contains("得標廠商", na=False))
            & (company_data["item_content"] == "決標金額")]

        # 提取并清洗金额数据
        amounts = winning_amounts["item_value"].str.replace(r"\D",
                                                            "",
                                                            regex=True)
        amounts = pd.to_numeric(amounts, errors="coerce")
        total_winning_amount = amounts.sum()

        # 计算“決標金額”与“底價金額”之间的差额，并取平均值
        # 筛选出有“決標金額”和“底價金額”的记录
        awarded_items = company_data[
            (company_data["item_key"].str.contains("得標廠商", na=False))
            & (company_data["item_content"].isin(["決標金額", "底價金額"]))]

        # 将数据透视，获取每个品项的“決標金額”和“底價金額”
        awarded_pivot = awarded_items.pivot_table(
            index=["item_key"],
            columns="item_content",
            values="item_value",
            aggfunc="first",
        ).reset_index()

        # 清洗金额数据，仅保留数字字符
        for col in ["決標金額", "底價金額"]:
            if col in awarded_pivot.columns:
                awarded_pivot[col] = awarded_pivot[col].str.replace(r"\D",
                                                                    "",
                                                                    regex=True)
                awarded_pivot[col] = pd.to_numeric(awarded_pivot[col],
                                                   errors="coerce")

        # 计算差额
        if "決標金額" in awarded_pivot.columns and "底價金額" in awarded_pivot.columns:
            awarded_pivot["差額"] = (awarded_pivot["決標金額"] -
                                   awarded_pivot["底價金額"])
            # 计算平均差额
            average_difference = awarded_pivot["差額"].mean()
        else:
            average_difference = None  # 如果缺少数据，则设为 None

        # 将结果添加到结果列表中
        result_list.append({
            "ids": company_id,
            "names": company_name,
            "該標案中总得標品項數": total_winning_bids,
            "該標案中未得標品項數": total_losing_bids,
            "該標案中总得標金額數": total_winning_amount,
            "該標案中底價差額均值": average_difference,
        })

    # 将结果列表转换为 DataFrame
    df_summary = pd.DataFrame(result_list)
    """
    "投標廠商:投標廠商" 信息列表
    """
    dict_投標廠商_detail = item_record["detail"].copy()
    dict_投標廠商_detail = {
        key: value
        for key, value in dict_投標廠商_detail.items()
        if "投標廠商:投標廠商" in key and key.count(":") == 2
    }

    # 将字典转换为DataFrame
    df_投標廠商_detail = pd.DataFrame(list(dict_投標廠商_detail.items()),
                                  columns=["Key", "Value"])

    # 提取 key 中的最后一部分作为列名
    df_投標廠商_detail["Variable"] = df_投標廠商_detail["Key"].apply(
        lambda x: x.split(":")[-1])
    df_投標廠商_detail["Key"] = df_投標廠商_detail["Key"].apply(
        lambda x: x.rsplit(":", 1)[0])

    # 重构 DataFrame，使每个 "廠商" 作为一行，列名为提取出的变量
    df_投標廠商_detail = df_投標廠商_detail.pivot(index="Key",
                                          columns="Variable",
                                          values="Value")
    df_投標廠商_detail = df_投標廠商_detail.rename(columns=lambda x: "投標廠商信息_" + x)
    df_投標廠商_detail["ids"] = df_投標廠商_detail["投標廠商信息_廠商代碼"]
    """
    合并两类信息表
    """
    df_merge = pd.merge(left=df_summary,
                        right=df_投標廠商_detail,
                        on=["ids"],
                        how="outer")
    """
    添加基本信息
    """
    df_merge["date"] = item_record["date"]
    df_merge["filename"] = item_record["filename"]
    df_merge["filename"] = item_record["filename"]
    df_merge["title"] = item_record["brief"]["title"]
    df_merge["type"] = item_record["brief"]["type"]
    df_merge["unit_id"] = item_record["unit_id"]
    df_merge["job_number"] = item_record["job_number"]

    # 添加详细信息
    dict_detail = item_record["detail"].copy()
    dict_detail = {
        key.split(":", 1)[1] if ":" in key else key: value
        for key, value in dict_detail.items()
    }  # 规整detail的key
    for str_var in [
            "url",
            "招標方式",
            "決標方式",
            "標案名稱",
            "是否屬共同供應契約採購",
            "是否複數決標",
            "標的分類",
            "是否屬統包",
            "原公告日期",
            "原公告日期:remind",
            "預算金額是否公開",
            "預算金額",
            "是否受機關補助",
            "履約地點",
            "履約地點（含地區）",
            "是否含特別預算",
            "歸屬計畫類別",
            "投標廠商家數",
            "決標公告序號",
            "決標日期",
            "決標公告日期",
            "是否刊登公報",
            "底價金額",
            "底價金額是否公開",
            "總決標金額",
            "總決標金額是否公開",
            "契約是否訂有依物價指數調整價金規定",
            "漲跌幅調整幅度",
    ]:
        if str_var in dict_detail.keys():
            df_merge[str_var] = json.dumps(dict_detail[str_var],
                                           ensure_ascii=False)  # 以字符串存储

    df_merge["標案中縂決標品項數量"] = max([
        int(re.search(r"第(\d+)品項:", key).group(1))
        for key in dict_detail if re.search(r"第(\d+)品項:", key)
    ],
                                 default=None)

    # 结果
    return df_merge
