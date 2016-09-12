## libraries =====
# install.packages(c("tibble", "tidyr", purrr", "dplyr", "lubridate", "magrittr"))
library("magrittr")  # for pipe

## create a data frame =====
# similar to a SAS data step with datalines
blah_df <- tibble::tribble(
  ~id, ~date_var, ~datetime_var, ~double_var1, ~double_var2, ~numeric_var, ~logical_var, ~char_var, ~int_var,
  1L, "2016-01-02", "2016-01-02T11:11:04", "46.41", "68.48", "78.61", "true", 49, "77",
  2L, "2015-12-24", "2015-12-24T08:11:41", "118.11", "-248.99", "593.1", "true", 88, "84",
  3L, "2016-05-05", "2016-05-05T05:05:49", "84.68", "194", "123", "false", 104, "4949"
)

# structure
str(blah_df)

## purrr + dmap style ====
# i <3 purrr

# the code below may look quite verbose, however purrr::dmap_at makes more
#   sense when you want to bulk convert a list of columns
# for example, if you used dplyr::select_ to create a large character vector
#   of column variables you could pass the entire vector into .at
blah_df_purrr_dmap <- blah_df %>%
  purrr::dmap_at(.f = lubridate::ymd,
                 .at = "date_var") %>%
  purrr::dmap_at(.f = lubridate::ymd_hms,
                 .at = "datetime_var") %>%
  purrr::dmap_at(.f = as.double,
                 .at = c("double_var1", "double_var2", "numeric_var")) %>%
  purrr::dmap_at(.f = as.logical,
                 .at = "logical_var") %>%
  purrr::dmap_at(.f = as.character,
                 .at = "char_var") %>%
  purrr::dmap_at(.f = as.integer,
                 .at = "int_var")

str(blah_df_purrr_dmap)

## dplyr + mutate style =====
# i <3 dplyr
# looks like the most straightforward, however there is some repetition that
#   occurs when you have the same columns, double_var1 and double_var2, that
#   need to be converted to the same type
# for wide dataframes (those with a lot of columns), dplyr::mutate might become
#   much more verbose than something like purrr::dmap_at
blah_df_dplyr_mutate <- blah_df %>%
  dplyr::mutate(date_var = lubridate::ymd(date_var),
                datetime_var = lubridate::ymd_hms(datetime_var),
                double_var1 = as.double(double_var1),
                double_var2 = as.double(double_var2),
                numeric_var = as.double(numeric_var),
                logical_var = as.logical(logical_var),
                char_var = as.character(char_var),
                int_var = as.integer(int_var))

str(blah_df_dplyr_mutate)

## custom style =====
# i <3 closures

# Column metadata may be stored separately from the code needed to perform
#   the conversion. As an example, a dataframe with 2,000 columns could have
#   its metadata stored in a separate csv or xlsx file. The metadata file would
#   be read in as a dataframe and convert_cols would update the data types
#   en masse.
# convert_cols is trading the need to maintain a separate metadata file for
#   concise code that performs the data type conversion. The alternative is
#   to have verbose code that details each data type conversion to take place.
# convert_cols is a closure that contains two parts.
#   - An inner function, convert_, that converts a set of columns that are
#     to be converted to the same type using dmap_at.
#   - The enclosing environment that contains the dataframe that will be
#     updated and contains a call to invoke_rows to drive iteration.
# Finally, the function is missing cases to convert to the following types.
#   The types included should cover the most simple use cases.
#   - list
#   - factor
#   - raw
#   - complex

convert_cols <- function(df, types_df) {
  # trailing underscore represents a "private" function
  convert_ <- function(col_type, col_names, ...) {
    # swallow dots when using as part of invoke_rows
    # used in case types_df has other columns contained within
    dots <- list(...)

    # type check as well as setting the function to perform conversion
    if (col_type %in% c("character",
                        "date",
                        "datetime",
                        "double",
                        "integer",
                        "logical")) {
      conv_func <- switch(col_type,
                          "character" = as.character,
                          "date" = lubridate::ymd,
                          "datetime" = lubridate::ymd_hms,
                          "double" = as.double,
                          "integer" = as.integer,
                          "logical" = as.logical)
    } else if (col_type  %in% ("numeric")) {
      message("For col_type = 'numeric' utilizing type double")
      conv_func <- as.double
    } else {
      stop("col_type must be one of:\n    character, date, datetime, double, integer, or logical")
    }

    # col_names is a list, convert to a character vector
    col_names_chr <- col_names %>% purrr::flatten_chr()

    # name check
    # if not all columns to convert are in df_converted
    if (!all(tibble::has_name(df_converted, col_names_chr))) {
      # discard those columns that are not in df_converted
      col_names_chr_not <- purrr::discard(col_names_chr,
                                          tibble::has_name(df_converted,
                                                           col_names_chr))
      # keep just those columns that are in df_converted
      col_names_chr <- purrr::keep(col_names_chr,
                                   tibble::has_name(df_converted,
                                                    col_names_chr))
      message(paste0("Ignoring the following columns:\n  ",
                     paste(col_names_chr_not, collapse = "\n  "))
      )
    }
    # perform the conversion
    # because using a closure, able to use <<- to update df_converted
    df_converted <<- purrr::dmap_at(.d = df_converted,
                                    .at = col_names_chr,
                                    .f = conv_func)
  }

  # the dataframe to update
  df_converted <- df

  # utilize tidyr::nest so that conversions can happen in groups
  #   e.g. all character conversions happen at the same time in a single
  #     purrr::dmap_at call rather than one at a time
  types_df_nest <- types_df %>% tidyr::nest_("col_name", key_col = "col_names")

  # iterate over the types dataframe, which tells us how to convert columns
  #   in df
  # if there are columns within types_df other than col_name and col_type,
  #   they are swallowed by the fact that convert_ takes dots (...)
  purrr::invoke_rows(.d = types_df_nest,
                     .f = convert_)

  # return the new dataframe
  df_converted
}

# create a dataframe with columns named col_name and col_type (if you name
#   them something else you can always use dplyr::rename) with the column
#   name and the type you would like it converted to
type_conv_df <- tibble::tribble(
  ~col_name, ~col_type,
  "date_var", "date",
  "datetime_var", "datetime",
  "double_var1", "double",
  "double_var2", "double",
  "numeric_var", "numeric",
  "logical_var", "logical",
  "char_var", "character",
  "int_var", "integer"
)

blah_df_custom <- convert_cols(blah_df, type_conv_df)

str(blah_df_custom)

# demonstrate that if you mis-specify a column name, the function won't blow up
#   but will ignore that column
type_conv_df_with_missing <- tibble::tribble(
  ~col_name, ~col_type,
  "date_var", "date",
  "datetime_var", "datetime",
  "double_var1", "double",
  "double_var2", "double",
  "numeric_var", "numeric",
  "logical_var", "logical",
  "char_var", "character",
  "int_var", "integer",
  "not_a_var", "double"
)

blah_df_custom_with_missing <- convert_cols(blah_df, type_conv_df_with_missing)

## identical =====
# all combinations, should all evaluate to TRUE
# there is some transitive-ness that I'm ignoring
identical(blah_df_dplyr_mutate, blah_df_purrr_dmap)
identical(blah_df_dplyr_mutate, blah_df_custom)
identical(blah_df_dplyr_mutate, blah_df_custom_with_missing)
identical(blah_df_purrr_dmap, blah_df_custom)
identical(blah_df_purrr_dmap, blah_df_custom_with_missing)
identical(blah_df_custom, blah_df_custom_with_missing)
