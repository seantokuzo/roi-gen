import { User as UserIcon } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { cn } from '@/lib/utils'

export default function Header() {
  return (
    <header className="sticky top-0 z-30 flex h-16 items-center justify-between border-b border-border bg-card/80 px-6 backdrop-blur-sm">
      <div className="flex items-center gap-3">
        <MarketStatus />
        <Badge variant="outline" className="border-yellow-500/50 text-yellow-500">
          Paper Trading
        </Badge>
      </div>

      {/* Static placeholder — auth (Google OAuth → JWT) lands in a later phase */}
      <Avatar className="h-9 w-9">
        <AvatarFallback className="bg-primary/10 text-primary">
          <UserIcon className="h-4 w-4" />
        </AvatarFallback>
      </Avatar>
    </header>
  )
}

function MarketStatus() {
  // Simple market hours check (US Eastern, Mon-Fri, 9:30-16:00)
  const now = new Date()
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }))
  const day = eastern.getDay()
  const hours = eastern.getHours()
  const minutes = eastern.getMinutes()
  const totalMinutes = hours * 60 + minutes

  const isWeekday = day >= 1 && day <= 5
  const isMarketHours = totalMinutes >= 570 && totalMinutes < 960 // 9:30-16:00
  const isOpen = isWeekday && isMarketHours

  return (
    <div className="flex items-center gap-2 text-sm">
      <div
        className={cn(
          'h-2 w-2 rounded-full',
          isOpen ? 'bg-success animate-pulse' : 'bg-muted-foreground',
        )}
      />
      <span className="text-muted-foreground">
        Market {isOpen ? 'Open' : 'Closed'}
      </span>
    </div>
  )
}
