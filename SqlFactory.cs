/*
*
* SqlFactory.cs
*
* Copyright 2017 Yuichi Yoshii
*     吉井雄一 @ 吉井産業  you.65535.kir@gmail.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/
using System.Collections.Generic;

using QueryBuilder;
using SAXWrapper;

namespace SQLDI {
    public class SqlFactory {
        private string directory;
        public void SetDirectory(string arg) {
            directory = arg;
        }

        private string fileName;
        public void SetFileName(string arg) {
            fileName = arg;
        }

        private SettingReader setting;

        public SqlFactory() {
        }

        public void Prepare() {
            setting = new SettingReader();
            setting.SetDirectory(directory);
            setting.SetFileName(fileName);
            setting.Parse();
        }

        public int Count() {
            if (setting == null) {
                return 0;
            }
            return setting.GetNode().Find(@"SqlFactory").GetChildren().Count;
        }

        public string Name(int arg) {
            if (setting == null) {
                return string.Empty;
            }
            for (int i = 0; i < setting.GetNode().Find(@"SqlFactory").GetChildren().Count; i++) {
                if (i == arg) {
                    return setting.GetNode().Find(@"SqlFactory").GetChildren()[i].GetNodeName();
                }
            }
            return string.Empty;
        }

        public string ToString(string name) {
            NodeEntity root = setting.GetNode().Find(@"SqlFactory").Find(name);

            SelectStatement s = SelectFromNode(root);

            List<JoinKeyword> j = JoinsFromNode(root);

            WhereKeyword w = WhereFromNode(root);

            j.ForEach(item => { s.AddJoin(item); });
            s.SetWhere(w);

            return s.ToString();
        }

        private SelectStatement SelectFromNode(NodeEntity arg) {
            SelectStatement ret = new SelectStatement();
            NodeEntity columns = arg.Find(@"Select");
            for (int i = 0; i < columns.GetChildren().Count; i++) {
                NodeEntity child = columns.GetChildren()[i];
                if (i == 0) {
                    ret.SetTable(child.Find(@"Table").GetNodeValue());
                    ret.SetTableAlias(child.Find(@"Alias").GetNodeValue());
                }
                child.Find(@"Column").GetChildren().ForEach(item => {
                    ret.AddColumn(item.GetNodeValue(), child.Find(@"Alias").GetNodeValue());
                });
            }
            return ret;
        }

        private List<JoinKeyword> JoinsFromNode(NodeEntity arg) {
            List<JoinKeyword> ret = new List<JoinKeyword>();
            string tableName = arg.Find(@"Select").Find(@"Columns", @"attr", @"Main").Find(@"Table").GetNodeValue();
            string tableAlias = arg.Find(@"Select").Find(@"Columns", @"attr", @"Main").Find(@"Alias").GetNodeValue();
            NodeEntity joins = arg.Find(@"Join");
            joins.GetChildren().ForEach(item => {
                JoinKeyword add = new JoinKeyword();
                add.SetTable(tableName);
                add.SetTableAlias(tableAlias);
                add.SetJoinTable(item.Find(@"Name").GetNodeValue());
                add.SetJoinTableAlias(item.Find(@"Alias").GetNodeValue());
                add.SetInnerJoin(item.Find(@"Inner").GetNodeValue() == @"True" ? true : false);
                add.SetLeftOuterJoin(item.Find(@"Left").GetNodeValue() == @"True" ? true : false);
                add.SetRightOuterJoin(item.Find(@"Right").GetNodeValue() == @"True" ? true : false);
                add.SetCrossJoin(item.Find(@"Cross").GetNodeValue() == @"True" ? true : false);
                item.Find(@"Conditions").GetChildren().ForEach(c => {
                    add.AddCondition(
                        c.Find(@"Equal").GetNodeValue() == @"True" ? true : false
                      , c.Find(@"GreaterThanEqual").GetNodeValue() == @"True" ? true : false
                      , c.Find(@"JoinTableAsLarger").GetNodeValue() == @"True" ? true : false
                      , c.Find(@"JoinTableColumn").GetNodeValue()
                      , c.Find(@"TableColumn").GetNodeValue());
                });
                ret.Add(add);
            });
            return ret;
        }

        private WhereKeyword WhereFromNode(NodeEntity arg) {
            WhereKeyword ret = new WhereKeyword();
            NodeEntity where = arg.Find(@"Where");
            where.GetChildren().ForEach(item => {
                ret.AddCondition(
                    item.Find(@"Equal").GetNodeValue() == @"True" ? true : false
                  , item.Find(@"GreaterThanEqual").GetNodeValue() == @"True" ? true : false
                  , item.Find(@"RightSideAsLarger").GetNodeValue() == @"True" ? true : false
                  , item.Find(@"Alias").GetNodeValue()
                  , item.Find(@"Name").GetNodeValue()
                  , item.Find(@"Value").GetNodeValue());
            });
            return ret;
        }
    }
}
