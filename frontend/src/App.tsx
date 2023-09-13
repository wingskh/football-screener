import React, {
  useEffect,
  useState,
  MouseEvent,
  Dispatch,
  SetStateAction,
  ReactElement,
  useCallback,
} from "react";
import Box from "@mui/material/Box";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import { styled } from "@mui/material/styles";
import TableCell, { tableCellClasses } from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TableSortLabel from "@mui/material/TableSortLabel";
import Toolbar from "@mui/material/Toolbar";
import LoadingButton from "@mui/lab/LoadingButton";
import Typography from "@mui/material/Typography";
import Paper from "@mui/material/Paper";
import FormControlLabel from "@mui/material/FormControlLabel";
import Switch from "@mui/material/Switch";
import Refresh from "@mui/icons-material/Refresh";
import { visuallyHidden } from "@mui/utils";
import "./App.css";
import { io } from "socket.io-client";

interface Data {
  matchID: number;
  matchTime: string;
  league: string;
  homeTeam: string;
  awayTeam: string;
  homeOdds: GLfloat;
  handicap: string;
  awayOdds: GLfloat;
  diffSum: GLfloat;
  diffDetail: string;
  diffCount: GLfloat;
  reversedCount: string;
  reversedHome: string;
  reversedAway: string;
}

function createData(
  matchID: number,
  matchTime: string,
  league: string,
  homeTeam: string,
  awayTeam: string,
  homeOdds: GLfloat,
  handicap: string,
  awayOdds: GLfloat,
  diffSum: GLfloat,
  diffDetail: string,
  diffCount: GLfloat,
  reversedCount: string,
  reversedHome: string,
  reversedAway: string
): Data {
  return {
    matchID,
    matchTime,
    league,
    homeTeam,
    awayTeam,
    homeOdds,
    handicap,
    awayOdds,
    diffSum,
    diffDetail,
    diffCount,
    reversedCount,
    reversedHome,
    reversedAway,
  };
}

const StyledTableCell = styled(TableCell)(({ theme }): any => ({
  [`&.${tableCellClasses.head}`]: {
    backgroundColor: theme.palette.common.black,
    color: theme.palette.common.white,
  },
  [`&.${tableCellClasses.body}`]: {
    fontSize: 14,
  },
}));

const StyledTableRow = styled(TableRow)(({ theme }) => ({
  "&:nth-of-type(odd)": {
    backgroundColor: theme.palette.action.hover,
  },
  // hide last border
  "&:last-child td, &:last-child th": {
    border: 0,
  },
}));

const StyledTableSortLabel = styled(TableSortLabel)(({ theme }) => ({
  "&.MuiTableSortLabel-root:hover": {
    color: "#00ff00 !important",
    fontWeight: "bold !important",
  },
  "&.Mui-active": {
    color: "#00ff00 !important",
    fontWeight: "bold !important",
  },
  "& .MuiTableSortLabel-icon": {
    color: "#00ff00 !important",
    fontWeight: "bold !important",
  },
}));

function descendingComparator<T>(a: T, b: T, orderBy: keyof T) {
  if (b[orderBy] < a[orderBy]) {
    return -1;
  }
  if (b[orderBy] > a[orderBy]) {
    return 1;
  }
  return 0;
}

type Order = "asc" | "desc";

function getComparator<Key extends keyof any>(
  order: Order,
  orderBy: Key
): (
  a: { [key in Key]: number | string },
  b: { [key in Key]: number | string }
) => number {
  return order === "desc"
    ? (a, b) => descendingComparator(a, b, orderBy)
    : (a, b) => -descendingComparator(a, b, orderBy);
}

function stableSort<T>(
  array: readonly T[],
  comparator: (a: T, b: T) => number
) {
  const stabilizedThis = array.map((el, index) => [el, index] as [T, number]);
  stabilizedThis.sort((a, b) => {
    const order = comparator(a[0], b[0]);
    if (order !== 0) {
      return order;
    }
    return a[1] - b[1];
  });
  return stabilizedThis.map((el) => el[0]);
}

interface HeadCell {
  disablePadding: boolean;
  id: keyof Data;
  label: string;
  numeric: boolean;
}

const headCells: readonly HeadCell[] = [
  {
    id: "matchID",
    numeric: true,
    disablePadding: true,
    label: "Match ID",
  },
  {
    id: "matchTime",
    numeric: false,
    disablePadding: false,
    label: "Match Time",
  },
  {
    id: "league",
    numeric: false,
    disablePadding: false,
    label: "League",
  },
  {
    id: "homeTeam",
    numeric: false,
    disablePadding: false,
    label: "Home Team",
  },
  {
    id: "awayTeam",
    numeric: false,
    disablePadding: false,
    label: "Away Team",
  },
  {
    id: "homeOdds",
    numeric: true,
    disablePadding: false,
    label: "Home Odds",
  },
  {
    id: "handicap",
    numeric: false,
    disablePadding: false,
    label: "Handicap",
  },
  {
    id: "awayOdds",
    numeric: true,
    disablePadding: false,
    label: "Away Odds",
  },
  {
    id: "diffSum",
    numeric: true,
    disablePadding: false,
    label: "Diff Sum",
  },
  {
    id: "diffDetail",
    numeric: false,
    disablePadding: false,
    label: "Diff Detail",
  },
  {
    id: "diffCount",
    numeric: true,
    disablePadding: false,
    label: "Diff Count",
  },
  {
    id: "reversedCount",
    numeric: false,
    disablePadding: false,
    label: "Reversed Count",
  },
  {
    id: "reversedHome",
    numeric: false,
    disablePadding: false,
    label: "Reversed Home",
  },
  {
    id: "reversedAway",
    numeric: false,
    disablePadding: false,
    label: "Reversed Away",
  },
];

interface EnhancedTableProps {
  onRequestSort: (event: MouseEvent<unknown>, property: keyof Data) => void;
  order: Order;
  orderBy: string;
}

function EnhancedTableHead(props: EnhancedTableProps) {
  const { order, orderBy, onRequestSort } = props;
  const createSortHandler =
    (property: keyof Data) => (event: MouseEvent<unknown>) => {
      onRequestSort(event, property);
    };

  return (
    <TableHead>
      <StyledTableRow>
        {headCells.map((headCell) => (
          <StyledTableCell
            key={headCell.id}
            align="right"
            padding={headCell.disablePadding ? "none" : "normal"}
            sortDirection={orderBy === headCell.id ? order : false}
          >
            <StyledTableSortLabel
              active={orderBy === headCell.id}
              direction={orderBy === headCell.id ? order : "asc"}
              onClick={createSortHandler(headCell.id)}
            >
              {headCell.label}
              {orderBy === headCell.id ? (
                <Box component="span" sx={visuallyHidden}>
                  {order === "desc" ? "sorted descending" : "sorted ascending"}
                </Box>
              ) : null}
            </StyledTableSortLabel>
          </StyledTableCell>
        ))}
      </StyledTableRow>
    </TableHead>
  );
}
export default function App() {
  const [order, setOrder] = useState<Order>("asc");
  const [orderBy, setOrderBy] = useState<keyof Data>("matchTime");
  const [dense, setDense] = useState(false);
  const [odds, setOdds] = useState([]);
  const [isRefreshing, setIsRefreshing] = useState<boolean>(false);
  // localhost
  // const socket = io("http://127.0.0.1:5000", {
  //   reconnection: true,
  // });
  // Heroku
  const socket = io();

  const get_odds = useCallback(() => {
    console.log("SocketIO: emit get_odds");
    socket.emit("get_odds");
  }, [socket]);

  const handleRefreshClick = (setIsRefreshing: any) => {
    setIsRefreshing(true);
    get_odds();
  };

  const EnhancedTableToolbar = ({
    isRefreshing,
    setIsRefreshing,
  }: {
    isRefreshing: boolean;
    setIsRefreshing: Dispatch<SetStateAction<boolean>>;
  }): ReactElement => {
    return (
      <Toolbar
        sx={{
          pl: { sm: 2 },
          pr: { xs: 1, sm: 1 },
        }}
      >
        <Typography> </Typography>
        <Typography
          sx={{ flex: "1 1 100%" }}
          variant="h6"
          id="tableTitle"
          component="div"
        >
          Football Screener
        </Typography>
        <LoadingButton
          loading={isRefreshing}
          loadingPosition="start"
          startIcon={<Refresh />}
          variant="outlined"
          onClick={() => handleRefreshClick(setIsRefreshing)}
        >
          Refresh
        </LoadingButton>
      </Toolbar>
    );
  };

  useEffect(() => {
    socket.on("receive_odds", (rawOdds) => {
      console.log("SocketIO: emit receive_odds", rawOdds);
      setOdds(
        JSON.parse(rawOdds).map((match: any) => {
          return createData(
            match["matchID"],
            match["matchTime"],
            match["league"],
            match["homeTeam"],
            match["awayTeam"],
            match["homeOdds"],
            match["handicap"],
            match["awayOdds"],
            match["diffSum"],
            match["diffDetail"],
            match["diffCount"],
            match["reversedCount"],
            match["reversedHome"],
            match["reversedAway"]
          );
        })
      );
      setIsRefreshing(false);
    });

    get_odds();
  }, [get_odds, socket]);

  const handleRequestSort = (
    event: MouseEvent<unknown>,
    property: keyof Data
  ) => {
    const isAsc = orderBy === property && order === "asc";
    setOrder(isAsc ? "desc" : "asc");
    setOrderBy(property);
  };

  const handleChangeDense = (event: React.ChangeEvent<HTMLInputElement>) => {
    setDense(event.target.checked);
  };

  return (
    <Box sx={{ width: "100%" }}>
      <Box sx={{ width: "90%" }} className="table">
        <Paper sx={{ width: "100%", mb: 2 }}>
          <EnhancedTableToolbar
            isRefreshing={isRefreshing}
            setIsRefreshing={setIsRefreshing}
          />
          <TableContainer>
            <Table
              sx={{ minWidth: 750 }}
              aria-labelledby="tableTitle"
              size={dense ? "small" : "medium"}
            >
              <EnhancedTableHead
                order={order}
                orderBy={orderBy}
                onRequestSort={handleRequestSort}
              />
              <TableBody>
                {stableSort(odds, getComparator(order, orderBy)).map(
                  (row, index) => {
                    const labelId = `enhanced-table-checkbox-${index}`;

                    return (
                      <StyledTableRow hover tabIndex={-1} key={row.matchID}>
                        <StyledTableCell id={labelId} scope="row" align="right">
                          {row.matchID}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.matchTime}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.league}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.homeTeam}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.awayTeam}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.homeOdds}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.handicap}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.awayOdds}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.diffSum}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          <pre>{row.diffDetail}</pre>
                        </StyledTableCell>
                        <StyledTableCell
                          align="right"
                          style={{ wordBreak: "keep-all" }}
                        >
                          {row.diffCount}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          {row.reversedCount}
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          <pre>{row.reversedHome}</pre>
                        </StyledTableCell>
                        <StyledTableCell align="right">
                          <pre>{row.reversedAway}</pre>
                        </StyledTableCell>
                      </StyledTableRow>
                    );
                  }
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </Paper>
        <FormControlLabel
          control={<Switch checked={dense} onChange={handleChangeDense} />}
          label="Dense padding"
        />
      </Box>
    </Box>
  );
}
