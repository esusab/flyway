package org.flywaydb.core;

@SuppressWarnings("unused")
public class EsusFlywayTables {
	private static final String version_rank = "NU_VERSAO";
	private static final String installed_rank = "NU_VERSAO_INSTALADA";
	private static final String version = "NO_VERSAO";
	private static final String description = "NO_DESCRICAO";
	private static final String type = "NO_TIPO";
	private static final String script = "NO_SCRIPT";
	private static final String checksum = "NU_MD5_SOMA_VERIFICACAO";
	private static final String installed_by = "NO_INSTALADO_POR";
	private static final String installed_on = "DT_INSTALACAO";
	private static final String execution_time = "NU_TEMPO_EXECUCAO";
	private static final String success = "ST_SUCESSO";

	public static String getVersionRank() {
		return "NU_VERSAO";
	}

	public static String getInstalledRank() {
		return "NU_VERSAO_INSTALADA";
	}

	public static String getVersion() {
		return "NO_VERSAO";
	}

	public static String getDescription() {
		return "NO_DESCRICAO";
	}

	public static String getType() {
		return "NO_TIPO";
	}

	public static String getScript() {
		return "NO_SCRIPT";
	}

	public static String getChecksum() {
		return "NU_MD5_SOMA_VERIFICACAO";
	}

	public static String getInstalledBy() {
		return "NO_INSTALADO_POR";
	}

	public static String getInstalledOn() {
		return "DT_INSTALACAO";
	}

	public static String getExecutionTime() {
		return "NU_TEMPO_EXECUCAO";
	}

	public static String getSuccess() {
		return "ST_SUCESSO";
	}
}
