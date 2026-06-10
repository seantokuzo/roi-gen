import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  Briefcase,
  Brain,
  ArrowLeftRight,
  Radio,
  MessageSquare,
  TrendingUp,
} from 'lucide-react'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { cn } from '@/lib/utils'

// Only "/" exists in Phase 0 — the rest land in Phase 5 (catch-all redirects home).
const navItems = [
  { to: '/', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/portfolio', icon: Briefcase, label: 'Portfolio' },
  { to: '/strategies', icon: Brain, label: 'Strategies' },
  { to: '/trades', icon: ArrowLeftRight, label: 'Trades' },
  { to: '/signals', icon: Radio, label: 'Signals' },
  { to: '/chat', icon: MessageSquare, label: 'Chat' },
]

export default function Sidebar() {
  return (
    <aside className="group fixed left-0 top-0 z-40 flex h-screen w-[72px] flex-col border-r border-border bg-card transition-all duration-300 hover:w-[200px]">
      {/* Logo */}
      <div className="flex h-16 items-center gap-3 overflow-hidden px-5">
        <TrendingUp className="h-6 w-6 shrink-0 text-primary" />
        <span className="whitespace-nowrap text-lg font-bold text-primary opacity-0 transition-opacity duration-300 group-hover:opacity-100">
          ROI-GEN
        </span>
      </div>

      {/* Nav */}
      <nav className="mt-4 flex flex-1 flex-col gap-1 px-3">
        {navItems.map(({ to, icon: Icon, label }) => (
          <Tooltip key={to} delayDuration={0}>
            <TooltipTrigger asChild>
              <NavLink
                to={to}
                end={to === '/'}
                className={({ isActive }) =>
                  cn(
                    'flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors',
                    'hover:bg-accent hover:text-accent-foreground',
                    isActive
                      ? 'bg-primary/10 text-primary'
                      : 'text-muted-foreground',
                  )
                }
              >
                <Icon className="h-5 w-5 shrink-0" />
                <span className="whitespace-nowrap opacity-0 transition-opacity duration-300 group-hover:opacity-100">
                  {label}
                </span>
              </NavLink>
            </TooltipTrigger>
            <TooltipContent side="right" className="group-hover:hidden">
              {label}
            </TooltipContent>
          </Tooltip>
        ))}
      </nav>
    </aside>
  )
}
