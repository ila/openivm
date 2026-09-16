RequirePlotPackages <- function(packages) {
  missing <- packages[!vapply(packages, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(missing) > 0) {
    stop(
      "Missing R packages: ", paste(missing, collapse = ", "),
      ". Install them before plotting; paper scripts do not modify the R environment."
    )
  }
}

PaperFont <- function() {
  if (!requireNamespace("systemfonts", quietly = TRUE)) {
    return("serif")
  }
  tryCatch({
    families <- systemfonts::system_fonts()$family
    if (any(grepl("Linux Libertine", families, fixed = TRUE))) "Linux Libertine" else "serif"
  }, error = function(e) "serif")
}
