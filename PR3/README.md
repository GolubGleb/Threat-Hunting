---
title: "Анализ данных сетевого трафика при помощи библиотеки Arrow"
author: "Тыван Максим"
format:
    md: 
        output-file: README
engine: knitr
---

# Цель работы

1.  Изучить возможности технологии Apache Arrow для обработки и анализ больших данных
2.  Получить навыки применения Arrow совместно с языком программирования R
3.  Получить навыки анализа метаинфомации о сетевом трафике
4.  Получить навыки применения облачных технологий хранения, подготовки и анализа данных: Yandex Object Storage, Rstudio Server.

```{r}
library(tidyverse)
library(dplyr)
library(arrow)
```

## Задание 1

Важнейшие документы с результатами нашей исследовательской деятельности в области создания вакцин скачиваются в виде больших заархивированных дампов. Один из хостов в нашей сети используется для пересылки этой информации – он пересылает гораздо больше информации на внешние ресурсы в Интернете, чем остальные компьютеры нашей сети. Определите его IP-адрес.

```{r}
leak <- df %>% select(src, bytes) %>% group_by(src) %>% summarise(bytsum = sum(bytes)) %>% arrange(desc(bytsum)) %>% head(1)

leak %>% collect
```

Ответ: 13.37.84.125

## Задание 2

Другой атакующий установил автоматическую задачу в системном планировщике cron для экспорта содержимого внутренней wiki системы. Эта система генерирует большое количество трафика в нерабочие часы, больше чем остальные хосты. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителя из предыдущей задачи.

Находим нерабочее время

```{r}
library(lubridate)
time <- df %>% select(timestamp, src, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% group_by(timestamp) %>% summarise(traffic_size = sum(bytes)) %>% arrange(desc(traffic_size))

time %>% collect()
```

По количеству передаваемых данных видим, что с 0 до 15 часов трафик значительно ниже и это вероятнее всего нерабочие часы. Теперь найдём IP зараженного хоста.

```{r}
nw_time <- df %>% select(timestamp, src, bytes) %>% mutate(timestamp = hour(as_datetime(timestamp/1000))) %>% filter(src != '13.37.84.125' & timestamp >= 0 & timestamp <= 15) %>% group_by(src) %>% summarise(bytsum = sum(bytes)) %>% arrange(desc(bytsum)) %>% collect()

nw_time %>% head(1)
```

Ответ: 12.55.77.96

## Задание 3

Еще один нарушитель собирает содержимое электронной почты и отправляет в Интернет используя порт, который обычно используется для другого типа трафика. Атакующий пересылает большое количество информации используя этот порт, которое нехарактерно для других хостов, использующих этот номер порта. Определите IP этой системы. Известно, что ее IP адрес отличается от нарушителей из предыдущих задач.

Для начала сравним среднее и максимальное количество трафика каждого хоста по портам.

```{r}
sr_traf <- df %>% select(src, port, bytes) %>% group_by(src, port) %>% summarise(bytport_sum = sum(bytes)) %>% group_by(port) %>% summarise(sr_traf = mean(bytport_sum)) %>% arrange(desc(sr_traf))

sr_traf %>% collect()

max_traf <- df %>% select(src, port, bytes) %>% filter(src != '13.37.84.125' & src != '12.55.77.96') %>% group_by(src, port) %>% summarise(bytport_sum = sum(bytes)) %>% collect() %>% group_by(port) %>% top_n(1, bytport_sum) %>% arrange(desc(bytport_sum))

max_traf %>% collect()

#Теперь найдём разницу между средним и максимальным значением трафика по портам.

merged_df <- merge(max_traf, sr_traf, by = 'port')

bad_traf <- merged_df %>% mutate(bad_bytes = bytport_sum/sr_traf) %>% arrange(desc(bad_bytes)) %>% head(1)

bad_traf %>% collect()
```

Ответ: 12.30.96.87
