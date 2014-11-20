--
-- Copyright 2010-2014 Axel Fontaine
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--         http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE TABLE "${schema}"."${table}" (
    "NU_VERSAO" INT NOT NULL,
    "NU_VERSAO_INSTALADA" INT NOT NULL,
    "NO_VERSAO" VARCHAR2(50) NOT NULL,
    "NO_DESCRICAO" VARCHAR2(200) NOT NULL,
    "NO_TIPO" VARCHAR2(20) NOT NULL,
    "NO_SCRIPT" VARCHAR2(1000) NOT NULL,
    "NU_MD5_SOMA_VERIFICACAO" INT,
    "NO_INSTALADO_POR" VARCHAR2(100) NOT NULL,
    "DT_INSTALACAO" TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    "NU_TEMPO_EXECUCAO" INT NOT NULL,
    "ST_SUCESSO" NUMBER(1) NOT NULL
);
ALTER TABLE ${schema}.${table} ADD CONSTRAINT PK_MIGRACAODADOS PRIMARY KEY (NO_VERSAO);

CREATE INDEX IN_MIGRACAO_ESTRUTURA_NUVERSAO ON ${schema}.${table} (NU_VERSAO);
CREATE INDEX IN_MIGRACAO_ESTRUTR_NVRSINSTLD ON ${schema}.${table} (NU_VERSAO_INSTALADA);
CREATE INDEX IN_MIGRACAO_ESTRUTURA_ST_SUCSS ON ${schema}.${table} (ST_SUCESSO);
