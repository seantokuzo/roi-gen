import {
  Activity,
  DollarSign,
  TrendingUp,
  Wallet,
  type LucideIcon,
} from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'

interface StatPlaceholder {
  label: string
  value: string
  hint: string
  icon: LucideIcon
}

const stats: StatPlaceholder[] = [
  { label: 'Portfolio Value', value: '—', hint: 'Equity across portfolios', icon: Wallet },
  { label: 'Day P&L', value: '—', hint: 'Realized + unrealized (ET day)', icon: TrendingUp },
  { label: 'Open Positions', value: '—', hint: 'Across active strategies', icon: Activity },
  { label: 'Buying Power', value: '—', hint: 'Margin-headroom guarded', icon: DollarSign },
]

export default function Dashboard() {
  return (
    <div className="space-y-6">
      {/* Phase 0 banner */}
      <div className="flex items-center justify-between rounded-lg border border-border bg-card px-5 py-4">
        <div>
          <h1 className="text-xl font-semibold">Dashboard</h1>
          <p className="text-sm text-muted-foreground">
            Scaffold only — live data, charts, and controls land in Phase 5.
          </p>
        </div>
        <Badge variant="outline" className="border-primary/50 text-primary">
          Phase 0
        </Badge>
      </div>

      {/* KPI placeholders */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {stats.map(({ label, value, hint, icon: Icon }) => (
          <Card key={label} className="gap-2 py-5">
            <CardHeader className="pb-0">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {label}
              </CardTitle>
              <Icon className="col-start-2 row-start-1 h-4 w-4 justify-self-end text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="font-mono text-2xl font-bold">{value}</div>
              <p className="mt-1 text-xs text-muted-foreground">{hint}</p>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}
