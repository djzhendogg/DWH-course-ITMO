#!/usr/bin/env bash
log "Создаём директорию ${HDFS}/createme"
${HADOOP_BIN} dfs -mkdir -p ${HDFS}/createme
if ${HADOOP_BIN} dfs -test -d ${HDFS}/createme; then
log "OK: /createme создана"
else
echo "ERROR: не удалось создать /createme" >&2
exit 1
fi


# 2) Удаление директории /delme (если есть) — сначала создадим, затем удалим, чтобы проверить работу удаления
log "Создаём и удаляем директорию ${HDFS}/delme"
${HADOOP_BIN} dfs -mkdir -p ${HDFS}/delme
${HADOOP_BIN} dfs -rm -r -f ${HDFS}/delme || true
if ${HADOOP_BIN} dfs -test -d ${HDFS}/delme; then
echo "ERROR: /delme всё ещё существует" >&2
exit 1
else
log "OK: /delme удалена"
fi


# 3) Создание файла /nonnull.txt с произвольным содержимым
log "Создаём временный файл и отправляем в ${HDFS}/nonnull.txt"
TMPFILE=$(mktemp)
echo "This is a non-null test file. Generated at $(date -u)." > ${TMPFILE}
${HADOOP_BIN} dfs -put -f ${TMPFILE} ${HDFS}/nonnull.txt
${HADOOP_BIN} dfs -test -e ${HDFS}/nonnull.txt || { echo "ERROR: не удалось записать /nonnull.txt" >&2; exit 1; }
log "OK: /nonnull.txt записан"
rm -f ${TMPFILE}


# 4) Выполнить джобу MR wordcount через YARN для файла /shadow.txt
# Очистим возможный старый результат
OUTPUT_DIR=${HDFS}/wordcount-output-$(date +%s)
log "Запуск MapReduce wordcount для ${HDFS}/shadow.txt -> ${OUTPUT_DIR}"


# Запуск джобы
${YARN_BIN} jar ${HADOOP_EXAMPLES_JAR} wordcount ${HDFS}/shadow.txt ${OUTPUT_DIR}


# Ждём завершения (yarn jar запускает и ждёт)
# Проверим, что результат есть
${HADOOP_BIN} dfs -test -d ${OUTPUT_DIR} || { echo "ERROR: output dir ${OUTPUT_DIR} отсутствует" >&2; exit 1; }
log "OK: WordCount завершён, результат в ${OUTPUT_DIR}"


# 5) Записать число вхождений слова "Innsmouth" (именно в такой форме) в файл /whataboutinsmouth.txt
log "Ищем счётчик слова 'Innsmouth' в выводе wordcount"


# Соберём output/part-* в локальный временный файл и посчитаем
TMP_OUT=$(mktemp)
${HADOOP_BIN} dfs -cat ${OUTPUT_DIR}/part-* > ${TMP_OUT} || true


# Ищем строку, где слово точно такое "Innsmouth" (без кавычек) — счетчик обычно хранится как: Innsmouth\t<count>
COUNT=$(grep -P "^Innsmouth\s+" ${TMP_OUT} | awk '{print $2}' || true)


if [ -z "${COUNT}" ]; then
# Если grep не нашёл — записываем 0 как безопасное значение
COUNT=0
log "Внимание: слово 'Innsmouth' не найдено в выводе wordcount; заместительное значение 0 будет записано"
else
log "Найдено: Innsmouth -> ${COUNT}"
fi


# Записываем значение в HDFS: /whataboutinsmouth.txt
echo "${COUNT}" > ${TMP_OUT}
${HADOOP_BIN} dfs -put -f ${TMP_OUT} ${HDFS}/whataboutinsmouth.txt
${HADOOP_BIN} dfs -test -e ${HDFS}/whataboutinsmouth.txt || { echo "ERROR: не удалось записать /whataboutinsmouth.txt" >&2; exit 1; }
log "OK: /whataboutinsmouth.txt записан со значением: ${COUNT}"
rm -f ${TMP_OUT}


log "ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ УСПЕШНО"


# Для удобства — выводим содержимое всех ключевых путей
log "--- Итоговый список HDFS (root) ---"
${HADOOP_BIN} dfs -ls ${HDFS} || true


exit 0